package operations

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDueBoundaryAndDiagnosisPrecedence(t *testing.T) {
	deadline := time.Date(2026, 10, 7, 12, 0, 30, 0, time.UTC)
	input := ClassificationInput{SourceFinal: true, RelayEligible: true, ObservationReady: true, ExpectedBy: deadline}
	input.Now = deadline.Add(-time.Nanosecond)
	if Due(input) {
		t.Fatal("early classification")
	}
	input.Now = deadline
	if !Due(input) || Diagnose(input) != DiagnosisStuck {
		t.Fatal("exact deadline is due")
	}
	input.Now = deadline.Add(time.Nanosecond)
	if !Due(input) {
		t.Fatal("late classification")
	}
	input.ManualIntervention = true
	input.Conflict = true
	input.DuplicateExecution = true
	input.ObservationReady = false
	if Diagnose(input) != DiagnosisManualIntervention || Due(input) {
		t.Fatal("manual precedence")
	}
	input.ManualIntervention = false
	if Diagnose(input) != DiagnosisConflict {
		t.Fatal("conflict precedence")
	}
	input.Conflict = false
	if Diagnose(input) != DiagnosisDuplicate {
		t.Fatal("duplicate precedence")
	}
	input.DuplicateExecution = false
	if Diagnose(input) != DiagnosisUnavailable {
		t.Fatal("outage precedence")
	}
	input.ObservationReady = true
	input.DestinationSeen = true
	if Due(input) || Diagnose(input) != DiagnosisOrdinary {
		t.Fatal("destination evidence blocks stuck")
	}
}

func TestWebhookDeliveryBoundary(t *testing.T) {
	var received int
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received++
		if r.Method != http.MethodPost || r.Header.Get("Idempotency-Key") != "stable" || r.ContentLength > 4096 {
			t.Error("invalid alert request")
		}
		switch received {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer endpoint.Close()
	sink, err := NewWebhookSink(endpoint.URL)
	if err != nil {
		t.Fatal(err)
	}
	alert := Alert{ID: 1, Kind: "STUCK", IdempotencyKey: "stable"}
	if err = sink.Deliver(context.Background(), alert); err == nil {
		t.Fatal("server failure accepted")
	}
	if err = sink.Deliver(context.Background(), alert); err == nil {
		t.Fatal("client failure accepted")
	} else {
		var permanent PermanentError
		if !errors.As(err, &permanent) {
			t.Fatal("client error must be permanent")
		}
	}
	if err = sink.Deliver(context.Background(), alert); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"http://example.com/alert", "https://user:secret@example.com/alert", "https://example.com/alert?token=secret", "ftp://example.com/alert"} {
		if _, err = NewWebhookSink(raw); err == nil {
			t.Fatalf("unsafe URL accepted: %s", raw)
		}
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, endpoint.URL, http.StatusFound) }))
	defer redirect.Close()
	redirectSink, err := NewWebhookSink(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err = redirectSink.Deliver(context.Background(), alert); err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("redirect followed: %v", err)
	}
	if received != 3 {
		t.Fatal("redirect reached target")
	}
}
