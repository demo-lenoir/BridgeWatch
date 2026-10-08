package operations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

// WebhookSink sends a bounded alert to one operator-configured endpoint.
type WebhookSink struct {
	url    string
	client *http.Client
}

func NewWebhookSink(rawURL string) (*WebhookSink, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid webhook URL")
	}
	if u.Scheme != "https" {
		host := u.Hostname()
		if u.Scheme != "http" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return nil, errors.New("webhook requires HTTPS or loopback HTTP")
		}
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &WebhookSink{url: u.String(), client: client}, nil
}

func (s *WebhookSink) Deliver(ctx context.Context, alert Alert) error {
	body, err := json.Marshal(alert)
	if err != nil || len(body) > 4096 {
		return PermanentError{Err: errors.New("invalid alert payload")}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return PermanentError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", alert.IdempotencyKey)
	response, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	statusErr := fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
	if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != http.StatusRequestTimeout && response.StatusCode != http.StatusTooManyRequests {
		return PermanentError{Err: statusErr}
	}
	return statusErr
}
