package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"bridgewatch/internal/destination"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/operations"
	"bridgewatch/internal/source"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type testClock struct{ at time.Time }

func (c testClock) Now() time.Time { return c.at }

type testSource struct{ state source.Status }

func (s *testSource) Ready(context.Context) (bool, error)           { return s.state.Ready, nil }
func (s *testSource) Status(context.Context) (source.Status, error) { return s.state, nil }

type testDestination struct{ state destination.Status }

func (s *testDestination) Ready(context.Context) (bool, error)                { return s.state.Ready, nil }
func (s *testDestination) Status(context.Context) (destination.Status, error) { return s.state, nil }

func TestReadOnlyAPIAndShutdown(t *testing.T) {
	url := os.Getenv("BRIDGEWATCH_TEST_DESTINATION_DB_URL")
	if url == "" {
		t.Skip("PostgreSQL not configured")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(ctx, `TRUNCATE chain_incidents,destination_reorgs,destination_streams,source_reorgs,source_streams,message_alerts,message_transitions,message_anomalies,observation_conflicts,message_claims,message_observations,cross_chain_messages,chain_checkpoints,chain_blocks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	id1, id2 := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	sender, recipient, payload := bytes.Repeat([]byte{3}, 20), bytes.Repeat([]byte{4}, 20), bytes.Repeat([]byte{5}, 32)
	var pk1, pk2 int64
	err = db.QueryRow(ctx, `INSERT INTO cross_chain_messages(protocol,source_chain_id,destination_chain_id,message_id,state,sender,recipient,payload_hash,source_final_at,relay_eligible_at,expected_by,relay_sla_ms,relay_policy_version) VALUES('mockbridge-v1',31337,31338,$1,'RELAY_PENDING',$2,$3,$4,$5,$5,$6,30000,1) RETURNING id`, id1, sender, recipient, payload, anchor, anchor.Add(30*time.Second)).Scan(&pk1)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(ctx, `INSERT INTO cross_chain_messages(protocol,source_chain_id,destination_chain_id,message_id,state) VALUES('mockbridge-v1',31337,31338,$1,'NO_CANONICAL_EVIDENCE') RETURNING id`, id2).Scan(&pk2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, `INSERT INTO message_anomalies(message_pk,kind,episode,reason,opened_at,sla_anchor,sla_threshold_ms,relay_policy_version) VALUES($1,'STUCK',1,'late',$2,$3,30000,1)`, pk1, anchor.Add(30*time.Second), anchor)
	if err != nil {
		t.Fatal(err)
	}
	sourceID, _ := domain.ChainIDFromDecimal("31337")
	destID, _ := domain.ChainIDFromDecimal("31338")
	src := &testSource{state: source.Status{ChainID: sourceID, Head: 5, Checkpoint: 5, Health: "HEALTHY", Ready: true, Confirmations: 3}}
	dst := &testDestination{state: destination.Status{ChainID: destID, Head: 5, Checkpoint: 5, Health: "HEALTHY", Ready: true, Confirmations: 3}}
	registry := prometheus.NewRegistry()
	metrics, err := operations.NewMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(db, testClock{anchor.Add(31 * time.Second)}, src, dst, registry, metrics)
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		server.Handler().ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		return r
	}
	messageKey := domain.MessageKey{Protocol: "mockbridge-v1", SourceChainID: sourceID, DestinationChainID: destID}
	copy(messageKey.MessageID[:], id1)
	response := get("/v1/messages/" + EncodeMessageID(messageKey))
	if response.Code != 200 {
		t.Fatalf("message: %d %s", response.Code, response.Body.String())
	}
	var message messageResponse
	if err = json.Unmarshal(response.Body.Bytes(), &message); err != nil {
		t.Fatal(err)
	}
	if message.State != domain.RelayPending || !message.Stuck || message.Diagnosis != operations.DiagnosisStuck || message.ExpectedBy == nil || message.AgeSeconds != 31 || len(message.Anomalies) != 1 {
		t.Fatalf("message evidence: %+v", message)
	}
	copy(messageKey.MessageID[:], bytes.Repeat([]byte{9}, 32))
	if get("/v1/messages/"+EncodeMessageID(messageKey)).Code != 404 {
		t.Fatal("missing message")
	}
	if get("/v1/messages/bad").Code != 400 || get("/v1/messages?limit=101").Code != 400 || get("/v1/messages?cursor=bad").Code != 400 {
		t.Fatal("invalid API input accepted")
	}
	first := get("/v1/messages?limit=1")
	if first.Code != 200 {
		t.Fatalf("list: %d %s", first.Code, first.Body.String())
	}
	var page struct {
		Items []messageResponse `json:"items"`
		Next  string            `json:"next_cursor"`
	}
	if err = json.Unmarshal(first.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Next == "" {
		t.Fatalf("first page: %s %v", first.Body.String(), err)
	}
	second := get("/v1/messages?limit=1&cursor=" + page.Next)
	if err = json.Unmarshal(second.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Next != "" {
		t.Fatalf("second page: %s %v", second.Body.String(), err)
	}
	for _, path := range []string{"/v1/messages?state=RELAY_PENDING", "/v1/messages?stuck=true", "/v1/messages?anomaly=STUCK"} {
		r := get(path)
		if r.Code != 200 {
			t.Fatalf("filter %s: %d %s", path, r.Code, r.Body.String())
		}
		if err = json.Unmarshal(r.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
			t.Fatalf("filter %s: %s %v", path, r.Body.String(), err)
		}
	}
	if r := get("/v1/anomalies?kind=STUCK&open=true"); r.Code != 200 || !strings.Contains(r.Body.String(), `"kind":"STUCK"`) {
		t.Fatalf("anomalies: %d %s", r.Code, r.Body.String())
	}
	if r := get("/v1/status"); r.Code != 200 || !strings.Contains(r.Body.String(), `"open_anomalies":1`) {
		t.Fatalf("status: %d %s", r.Code, r.Body.String())
	}
	if get("/health/live").Code != 200 || get("/health/ready").Code != 200 {
		t.Fatal("healthy probes")
	}
	if r := get("/metrics"); r.Code != 200 || !strings.Contains(r.Body.String(), "bridgewatch_stuck_messages") {
		t.Fatalf("metrics: %d %s", r.Code, r.Body.String())
	}
	dst.state.Ready = false
	if get("/health/ready").Code != 503 {
		t.Fatal("destination outage readiness")
	}
	dst.state.Ready = true
	dst.state.ManualIntervention = true
	dst.state.Health = "MANUAL_INTERVENTION"
	dst.state.Ready = false
	if get("/health/ready").Code != 503 {
		t.Fatal("manual intervention readiness")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx, listener) }()
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 20; i++ {
		res, e := client.Get("http://" + listener.Addr().String() + "/health/live")
		if e == nil {
			res.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP shutdown hung")
	}
	_ = pk2
}
