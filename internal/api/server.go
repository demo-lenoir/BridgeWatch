package api

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/destination"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/operations"
	"bridgewatch/internal/rpcpool"
	"bridgewatch/internal/source"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	db          *pgxpool.Pool
	clock       clock.Clock
	source      SourceMonitor
	destination DestinationMonitor
	registry    *prometheus.Registry
	metrics     *operations.Metrics
	handler     http.Handler
}

type SourceMonitor interface {
	Ready(context.Context) (bool, error)
	Status(context.Context) (source.Status, error)
}
type DestinationMonitor interface {
	Ready(context.Context) (bool, error)
	Status(context.Context) (destination.Status, error)
}

func New(db *pgxpool.Pool, businessClock clock.Clock, sourceWatcher SourceMonitor, destinationWatcher DestinationMonitor, registry *prometheus.Registry, metrics *operations.Metrics) (*Server, error) {
	if db == nil || businessClock == nil || sourceWatcher == nil || destinationWatcher == nil || registry == nil || metrics == nil {
		return nil, errors.New("API dependencies required")
	}
	s := &Server{db: db, clock: businessClock, source: sourceWatcher, destination: destinationWatcher, registry: registry, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/messages/{id}", s.getMessage)
	mux.HandleFunc("GET /v1/messages", s.listMessages)
	mux.HandleFunc("GET /v1/anomalies", s.listAnomalies)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live\n"))
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	scrape := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if err := s.metrics.Refresh(r.Context(), db); err != nil {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		scrape.ServeHTTP(w, r)
	})
	s.handler = mux
	return s, nil
}
func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) HTTPServer(address string) *http.Server {
	return &http.Server{Addr: address, Handler: s.handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 16}
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("listener required")
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	httpServer := s.HTTPServer(listener.Addr().String())
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-child.Done()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	err := httpServer.Serve(listener)
	cancel()
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func badRequest(w http.ResponseWriter, message string) {
	jsonResponse(w, http.StatusBadRequest, map[string]string{"error": message})
}
func unavailable(w http.ResponseWriter) {
	jsonResponse(w, http.StatusServiceUnavailable, map[string]string{"error": "storage or chain status unavailable"})
}
func hexValue(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(raw)
}

func EncodeMessageID(key domain.MessageKey) string {
	data := key.Protocol + "\x00" + key.SourceChainID.String() + "\x00" + key.DestinationChainID.String() + "\x00" + hex.EncodeToString(key.MessageID[:])
	return base64.RawURLEncoding.EncodeToString([]byte(data))
}
func decodeMessageID(value string) (domain.MessageKey, error) {
	if len(value) == 0 || len(value) > 256 {
		return domain.MessageKey{}, errors.New("invalid message identifier")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return domain.MessageKey{}, err
	}
	parts := strings.Split(string(data), "\x00")
	if len(parts) != 4 {
		return domain.MessageKey{}, errors.New("invalid message identity shape")
	}
	sourceID, err := domain.ChainIDFromDecimal(parts[1])
	if err != nil {
		return domain.MessageKey{}, err
	}
	destID, err := domain.ChainIDFromDecimal(parts[2])
	if err != nil {
		return domain.MessageKey{}, err
	}
	message, err := hex.DecodeString(parts[3])
	if err != nil || len(message) != 32 {
		return domain.MessageKey{}, errors.New("invalid message hash")
	}
	var hash domain.Hash
	copy(hash[:], message)
	key := domain.MessageKey{Protocol: parts[0], SourceChainID: sourceID, DestinationChainID: destID, MessageID: hash}
	return key, key.Validate()
}
func pageLimit(value string) (int, error) {
	if value == "" {
		return 50, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil || number < 1 || number > 100 {
		return 0, errors.New("limit must be between 1 and 100")
	}
	return number, nil
}
func decodeCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	if len(value) > 64 {
		return 0, errors.New("cursor too long")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id < 1 {
		return 0, errors.New("invalid cursor")
	}
	return id, nil
}
func encodeCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}
func validChain(value string) error {
	if value == "" {
		return nil
	}
	_, err := domain.ChainIDFromDecimal(value)
	return err
}
func validState(value string) bool {
	if value == "" {
		return true
	}
	return (domain.Projection{State: domain.State(value)}).Validate() == nil
}
func validKind(value string) bool {
	switch value {
	case "", "STUCK", "FAILED", "REORGED", "DUPLICATE_EXECUTION", "CONFLICT", "MANUAL_INTERVENTION":
		return true
	}
	return false
}

type evidence struct {
	ObservationID   int64     `json:"observation_id"`
	ChainID         string    `json:"chain_id"`
	BlockNumber     int64     `json:"block_number"`
	BlockHash       string    `json:"block_hash"`
	TransactionHash string    `json:"tx_hash"`
	LogIndex        int64     `json:"log_index"`
	Canonical       bool      `json:"canonical"`
	ObservedAt      time.Time `json:"observed_at"`
}
type finality struct {
	Policy                string     `json:"policy"`
	Satisfied             bool       `json:"satisfied"`
	SatisfiedAt           *time.Time `json:"satisfied_at"`
	RequiredConfirmations *int64     `json:"required_confirmations,omitempty"`
	ObservedConfirmations *int64     `json:"observed_confirmations,omitempty"`
	PolicyVersion         *int64     `json:"policy_version,omitempty"`
	JustifyingHeadBlock   *int64     `json:"justifying_head_block,omitempty"`
	JustifyingHeadHash    string     `json:"justifying_head_hash,omitempty"`
}
type anomaly struct {
	EventID            int64      `json:"event_id"`
	Kind               string     `json:"kind"`
	Episode            *int64     `json:"episode,omitempty"`
	Reason             string     `json:"reason"`
	OpenedAt           time.Time  `json:"opened_at"`
	ResolvedAt         *time.Time `json:"resolved_at"`
	ResolutionReason   string     `json:"resolution_reason,omitempty"`
	Subject            string     `json:"subject"`
	MessageID          string     `json:"message_id,omitempty"`
	Protocol           string     `json:"protocol,omitempty"`
	SourceChainID      string     `json:"source_chain_id,omitempty"`
	DestinationChainID string     `json:"destination_chain_id,omitempty"`
	ChainID            string     `json:"chain_id,omitempty"`
	Role               string     `json:"role,omitempty"`
	SLAAnchor          *time.Time `json:"sla_anchor,omitempty"`
	SLAThresholdMS     *int64     `json:"sla_threshold_ms,omitempty"`
	PolicyVersion      *int64     `json:"relay_policy_version,omitempty"`
}
type chainStatus struct {
	ChainID            string           `json:"chain_id"`
	Role               string           `json:"role"`
	Health             string           `json:"health"`
	Head               uint64           `json:"observed_head_block"`
	Checkpoint         uint64           `json:"checkpoint_block"`
	Lag                uint64           `json:"lag_blocks"`
	Ready              bool             `json:"ready"`
	ManualIntervention bool             `json:"manual_intervention"`
	Confirmations      uint64           `json:"confirmations"`
	LastSuccessfulAt   *time.Time       `json:"last_successful_at"`
	Endpoints          []rpcpool.Status `json:"endpoints,omitempty"`
}
type messageResponse struct {
	ID                  string               `json:"id"`
	Protocol            string               `json:"protocol"`
	SourceChainID       string               `json:"source_chain_id"`
	DestinationChainID  string               `json:"destination_chain_id"`
	MessageID           string               `json:"message_id"`
	State               domain.State         `json:"state"`
	Revision            int64                `json:"revision"`
	Sender              string               `json:"sender,omitempty"`
	Recipient           string               `json:"recipient,omitempty"`
	PayloadHash         string               `json:"payload_hash,omitempty"`
	SourceFinality      finality             `json:"source_finality"`
	DestinationFinality finality             `json:"destination_finality"`
	RelayEligibleAt     *time.Time           `json:"relay_eligible_at"`
	ExpectedBy          *time.Time           `json:"expected_by"`
	RelaySLAMS          *int64               `json:"relay_sla_ms,omitempty"`
	RelayPolicyVersion  *int64               `json:"relay_policy_version,omitempty"`
	AgeSeconds          int64                `json:"age_seconds"`
	Stuck               bool                 `json:"stuck"`
	Diagnosis           operations.Diagnosis `json:"diagnosis"`
	SourceEvidence      []evidence           `json:"source_evidence"`
	DestinationEvidence []evidence           `json:"destination_evidence"`
	Anomalies           []anomaly            `json:"anomalies"`
	SourceStatus        chainStatus          `json:"source_status"`
	DestinationStatus   chainStatus          `json:"destination_status"`
	CreatedAt           time.Time            `json:"created_at"`
	UpdatedAt           time.Time            `json:"updated_at"`
}

func mapSource(status source.Status) chainStatus {
	return chainStatus{ChainID: status.ChainID.String(), Role: "source", Health: status.Health, Head: status.Head, Checkpoint: status.Checkpoint, Lag: status.Lag, Ready: status.Ready, ManualIntervention: status.ManualIntervention, Confirmations: status.Confirmations, LastSuccessfulAt: status.LastSuccessfulAt, Endpoints: status.Endpoints}
}
func mapDestination(status destination.Status) chainStatus {
	return chainStatus{ChainID: status.ChainID.String(), Role: "destination", Health: status.Health, Head: status.Head, Checkpoint: status.Checkpoint, Lag: status.Lag, Ready: status.Ready, ManualIntervention: status.ManualIntervention, Confirmations: status.Confirmations, LastSuccessfulAt: status.LastSuccessfulAt, Endpoints: status.Endpoints}
}

func (s *Server) statuses(ctx context.Context) (chainStatus, chainStatus, error) {
	sourceStatus, err := s.source.Status(ctx)
	if err != nil {
		return chainStatus{}, chainStatus{}, err
	}
	destStatus, err := s.destination.Status(ctx)
	if err != nil {
		return chainStatus{}, chainStatus{}, err
	}
	return mapSource(sourceStatus), mapDestination(destStatus), nil
}

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	key, err := decodeMessageID(r.PathValue("id"))
	if err != nil {
		badRequest(w, "invalid message identifier")
		return
	}
	var pk int64
	err = s.db.QueryRow(r.Context(), `SELECT id FROM cross_chain_messages WHERE protocol=$1 AND source_chain_id=$2::numeric AND destination_chain_id=$3::numeric AND message_id=$4`, key.Protocol, key.SourceChainID.String(), key.DestinationChainID.String(), key.MessageID[:]).Scan(&pk)
	if errors.Is(err, pgx.ErrNoRows) {
		jsonResponse(w, http.StatusNotFound, map[string]string{"error": "message not found"})
		return
	}
	if err != nil {
		unavailable(w)
		return
	}
	message, err := s.loadMessage(r.Context(), pk)
	if err != nil {
		unavailable(w)
		return
	}
	jsonResponse(w, http.StatusOK, message)
}

func (s *Server) loadMessage(ctx context.Context, pk int64) (messageResponse, error) {
	var out messageResponse
	var sourceText, destText string
	var messageID, sender, recipient, payload []byte
	var sourceFinal, destFinal, relayAt, expected sql.NullTime
	var sla, policy sql.NullInt64
	err := s.db.QueryRow(ctx, `SELECT protocol,source_chain_id::text,destination_chain_id::text,message_id,state,state_revision,sender,recipient,payload_hash,source_final_at,destination_final_at,relay_eligible_at,expected_by,relay_sla_ms,relay_policy_version,created_at,updated_at FROM cross_chain_messages WHERE id=$1`, pk).Scan(&out.Protocol, &sourceText, &destText, &messageID, &out.State, &out.Revision, &sender, &recipient, &payload, &sourceFinal, &destFinal, &relayAt, &expected, &sla, &policy, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return out, err
	}
	var hash domain.Hash
	copy(hash[:], messageID)
	sourceID, err := domain.ChainIDFromDecimal(sourceText)
	if err != nil {
		return out, err
	}
	destID, err := domain.ChainIDFromDecimal(destText)
	if err != nil {
		return out, err
	}
	out.ID = EncodeMessageID(domain.MessageKey{Protocol: out.Protocol, SourceChainID: sourceID, DestinationChainID: destID, MessageID: hash})
	out.SourceChainID = sourceText
	out.DestinationChainID = destText
	out.MessageID = hexValue(messageID)
	out.Sender = hexValue(sender)
	out.Recipient = hexValue(recipient)
	out.PayloadHash = hexValue(payload)
	out.SourceFinality = finality{Policy: "confirmations", Satisfied: sourceFinal.Valid}
	out.DestinationFinality = finality{Policy: "confirmations", Satisfied: destFinal.Valid}
	if sourceFinal.Valid {
		out.SourceFinality.SatisfiedAt = &sourceFinal.Time
	}
	if destFinal.Valid {
		out.DestinationFinality.SatisfiedAt = &destFinal.Time
	}
	if relayAt.Valid {
		out.RelayEligibleAt = &relayAt.Time
		age := s.clock.Now().Sub(relayAt.Time)
		if age > 0 {
			out.AgeSeconds = int64(age.Seconds())
		}
	}
	if expected.Valid {
		out.ExpectedBy = &expected.Time
	}
	if sla.Valid {
		out.RelaySLAMS = &sla.Int64
	}
	if policy.Valid {
		out.RelayPolicyVersion = &policy.Int64
	}
	err = s.loadFinalityEvidence(ctx, pk, &out)
	if err != nil {
		return out, err
	}
	out.SourceEvidence, out.DestinationEvidence, err = s.loadEvidence(ctx, pk)
	if err != nil {
		return out, err
	}
	out.Anomalies, err = s.loadMessageAnomalies(ctx, pk)
	if err != nil {
		return out, err
	}
	out.SourceStatus, out.DestinationStatus, err = s.statuses(ctx)
	if err != nil {
		return out, err
	}
	input := operations.ClassificationInput{SourceFinal: sourceFinal.Valid, RelayEligible: relayAt.Valid, DestinationSeen: out.State == domain.DestinationSeen || out.State == domain.Completed, ObservationReady: out.SourceStatus.Ready && out.DestinationStatus.Ready, ManualIntervention: out.SourceStatus.ManualIntervention || out.DestinationStatus.ManualIntervention}
	for _, episode := range out.Anomalies {
		if episode.ResolvedAt != nil {
			continue
		}
		switch episode.Kind {
		case "STUCK":
			input.AlreadyStuck = true
			out.Stuck = true
		case "CONFLICT":
			input.Conflict = true
		case "DUPLICATE_EXECUTION":
			input.DuplicateExecution = true
		}
	}
	out.Diagnosis = operations.Diagnose(input)
	return out, nil
}

func (s *Server) loadFinalityEvidence(ctx context.Context, pk int64, out *messageResponse) error {
	var sh, dh []byte
	var sn, dn, sr, so, dr, do, sv, dv sql.NullInt64
	err := s.db.QueryRow(ctx, `SELECT justifying_head_hash,destination_head_hash,justifying_head_number,destination_head_number,source_required_confirmations,source_observed_confirmations,destination_required_confirmations,destination_observed_confirmations,source_policy_version,destination_policy_version FROM message_transitions WHERE message_pk=$1 ORDER BY revision DESC LIMIT 1`, pk).Scan(&sh, &dh, &sn, &dn, &sr, &so, &dr, &do, &sv, &dv)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	out.SourceFinality.JustifyingHeadHash = hexValue(sh)
	out.DestinationFinality.JustifyingHeadHash = hexValue(dh)
	if sn.Valid {
		out.SourceFinality.JustifyingHeadBlock = &sn.Int64
	}
	if dn.Valid {
		out.DestinationFinality.JustifyingHeadBlock = &dn.Int64
	}
	if sr.Valid {
		out.SourceFinality.RequiredConfirmations = &sr.Int64
	}
	if so.Valid {
		out.SourceFinality.ObservedConfirmations = &so.Int64
	}
	if sv.Valid {
		out.SourceFinality.PolicyVersion = &sv.Int64
	}
	if dr.Valid {
		out.DestinationFinality.RequiredConfirmations = &dr.Int64
	}
	if do.Valid {
		out.DestinationFinality.ObservedConfirmations = &do.Int64
	}
	if dv.Valid {
		out.DestinationFinality.PolicyVersion = &dv.Int64
	}
	return nil
}

func (s *Server) loadEvidence(ctx context.Context, pk int64) ([]evidence, []evidence, error) {
	rows, err := s.db.Query(ctx, `SELECT mc.role,o.id,o.chain_id::text,b.block_number,b.block_hash,o.tx_hash,o.log_index,b.canonical,o.first_seen_at FROM message_claims mc JOIN message_observations o ON o.id=mc.observation_id JOIN chain_blocks b ON b.chain_id=o.chain_id AND b.block_hash=o.block_hash WHERE mc.message_pk=$1 ORDER BY o.id DESC LIMIT 100`, pk)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	sources, destinations := []evidence{}, []evidence{}
	for rows.Next() {
		var role string
		var item evidence
		var block, tx []byte
		if err = rows.Scan(&role, &item.ObservationID, &item.ChainID, &item.BlockNumber, &block, &tx, &item.LogIndex, &item.Canonical, &item.ObservedAt); err != nil {
			return nil, nil, err
		}
		item.BlockHash = hexValue(block)
		item.TransactionHash = hexValue(tx)
		if role == "SOURCE" {
			sources = append(sources, item)
		} else {
			destinations = append(destinations, item)
		}
	}
	return sources, destinations, rows.Err()
}

func (s *Server) loadMessageAnomalies(ctx context.Context, pk int64) ([]anomaly, error) {
	rows, err := s.db.Query(ctx, `SELECT event_id,kind,episode,reason,opened_at,resolved_at,resolution_reason,sla_anchor,sla_threshold_ms,relay_policy_version FROM message_anomalies WHERE message_pk=$1 ORDER BY event_id DESC LIMIT 100`, pk)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []anomaly{}
	for rows.Next() {
		var item anomaly
		var episode int64
		var resolved, anchor sql.NullTime
		var reason sql.NullString
		var threshold, version sql.NullInt64
		if err = rows.Scan(&item.EventID, &item.Kind, &episode, &item.Reason, &item.OpenedAt, &resolved, &reason, &anchor, &threshold, &version); err != nil {
			return nil, err
		}
		item.Subject = "MESSAGE"
		item.Episode = &episode
		if resolved.Valid {
			item.ResolvedAt = &resolved.Time
		}
		if reason.Valid {
			item.ResolutionReason = reason.String
		}
		if anchor.Valid {
			item.SLAAnchor = &anchor.Time
		}
		if threshold.Valid {
			item.SLAThresholdMS = &threshold.Int64
		}
		if version.Valid {
			item.PolicyVersion = &version.Int64
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		unavailable(w)
		return
	}
	sourceReady, err := s.source.Ready(r.Context())
	if err != nil {
		unavailable(w)
		return
	}
	destReady, err := s.destination.Ready(r.Context())
	if err != nil {
		unavailable(w)
		return
	}
	if !sourceReady || !destReady {
		unavailable(w)
		return
	}
	jsonResponse(w, http.StatusOK, map[string]bool{"ready": true})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	sourceStatus, destStatus, err := s.statuses(r.Context())
	if err != nil {
		unavailable(w)
		return
	}
	var anomalies, pending int64
	err = s.db.QueryRow(r.Context(), `SELECT (SELECT count(*) FROM message_anomalies WHERE resolved_at IS NULL)+(SELECT count(*) FROM chain_incidents WHERE resolved_at IS NULL),(SELECT count(*) FROM message_alerts WHERE status='PENDING')`).Scan(&anomalies, &pending)
	if err != nil {
		unavailable(w)
		return
	}
	jsonResponse(w, http.StatusOK, map[string]any{"chains": []chainStatus{sourceStatus, destStatus}, "ready": sourceStatus.Ready && destStatus.Ready, "open_anomalies": anomalies, "alert_outbox_pending": pending})
}

func (s *Server) listMessages(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit, err := pageLimit(query.Get("limit"))
	if err != nil {
		badRequest(w, err.Error())
		return
	}
	cursor, err := decodeCursor(query.Get("cursor"))
	if err != nil {
		badRequest(w, "invalid cursor")
		return
	}
	protocol, sourceID, destID, state, stuckText, kind := query.Get("protocol"), query.Get("source_chain_id"), query.Get("destination_chain_id"), query.Get("state"), query.Get("stuck"), query.Get("anomaly")
	if len(protocol) > 64 || validChain(sourceID) != nil || validChain(destID) != nil || !validState(state) || !validKind(kind) {
		badRequest(w, "invalid filter")
		return
	}
	var stuck *bool
	if stuckText != "" {
		parsed, parseErr := strconv.ParseBool(stuckText)
		if parseErr != nil {
			badRequest(w, "invalid stuck filter")
			return
		}
		stuck = &parsed
	}
	rows, err := s.db.Query(r.Context(), `SELECT m.id FROM cross_chain_messages m WHERE m.id>$1 AND ($2='' OR m.protocol=$2)
		AND ($3='' OR m.source_chain_id=NULLIF($3,'')::numeric) AND ($4='' OR m.destination_chain_id=NULLIF($4,'')::numeric)
		AND ($5='' OR m.state=$5)
		AND ($6::boolean IS NULL OR EXISTS(SELECT 1 FROM message_anomalies a WHERE a.message_pk=m.id AND a.kind='STUCK' AND a.resolved_at IS NULL)=$6)
		AND ($7='' OR EXISTS(SELECT 1 FROM message_anomalies a WHERE a.message_pk=m.id AND a.kind=$7 AND a.resolved_at IS NULL))
		ORDER BY m.id LIMIT $8`, cursor, protocol, sourceID, destID, state, stuck, kind, limit+1)
	if err != nil {
		unavailable(w)
		return
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		unavailable(w)
		return
	}
	hasMore := len(ids) > limit
	if hasMore {
		ids = ids[:limit]
	}
	items := make([]messageResponse, 0, len(ids))
	for _, id := range ids {
		item, loadErr := s.loadMessage(r.Context(), id)
		if loadErr != nil {
			unavailable(w)
			return
		}
		items = append(items, item)
	}
	next := ""
	if hasMore {
		next = encodeCursor(ids[len(ids)-1])
	}
	jsonResponse(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s *Server) listAnomalies(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit, err := pageLimit(query.Get("limit"))
	if err != nil {
		badRequest(w, err.Error())
		return
	}
	cursor, err := decodeCursor(query.Get("cursor"))
	if err != nil {
		badRequest(w, "invalid cursor")
		return
	}
	kind, protocol, sourceID, destID := query.Get("kind"), query.Get("protocol"), query.Get("source_chain_id"), query.Get("destination_chain_id")
	if !validKind(kind) || len(protocol) > 64 || validChain(sourceID) != nil || validChain(destID) != nil {
		badRequest(w, "invalid filter")
		return
	}
	var open *bool
	if value := query.Get("open"); value != "" {
		parsed, parseErr := strconv.ParseBool(value)
		if parseErr != nil {
			badRequest(w, "invalid open filter")
			return
		}
		open = &parsed
	}
	rows, err := s.db.Query(r.Context(), `WITH events AS (
		SELECT a.event_id,a.kind,a.episode,a.reason,a.opened_at,a.resolved_at,a.resolution_reason,'MESSAGE'::text AS subject,m.message_id,m.protocol,m.source_chain_id::text AS source_id,m.destination_chain_id::text AS dest_id,''::text AS chain_id,''::text AS role,a.sla_anchor,a.sla_threshold_ms,a.relay_policy_version
		FROM message_anomalies a JOIN cross_chain_messages m ON m.id=a.message_pk
		UNION ALL
		SELECT ci.event_id,ci.kind,NULL::integer,ci.reason,ci.opened_at,ci.resolved_at,NULL::text,'CHAIN',NULL::bytea,''::text,
		CASE WHEN ci.role='SOURCE' THEN ci.chain_id::text ELSE '' END,
		CASE WHEN ci.role='DESTINATION' THEN ci.chain_id::text ELSE '' END,ci.chain_id::text,ci.role,NULL::timestamptz,NULL::bigint,NULL::integer FROM chain_incidents ci
	) SELECT event_id,kind,episode,reason,opened_at,resolved_at,resolution_reason,subject,message_id,protocol,source_id,dest_id,chain_id,role,sla_anchor,sla_threshold_ms,relay_policy_version
	FROM events WHERE event_id>$1 AND ($2='' OR kind=$2) AND ($3::boolean IS NULL OR (resolved_at IS NULL)=$3)
	AND ($4='' OR protocol=$4) AND ($5='' OR source_id=$5) AND ($6='' OR dest_id=$6)
	ORDER BY event_id LIMIT $7`, cursor, kind, open, protocol, sourceID, destID, limit+1)
	if err != nil {
		unavailable(w)
		return
	}
	defer rows.Close()
	items := []anomaly{}
	for rows.Next() {
		var item anomaly
		var episode, threshold, version sql.NullInt64
		var resolved, anchor sql.NullTime
		var reason sql.NullString
		var message []byte
		if err = rows.Scan(&item.EventID, &item.Kind, &episode, &item.Reason, &item.OpenedAt, &resolved, &reason, &item.Subject, &message, &item.Protocol, &item.SourceChainID, &item.DestinationChainID, &item.ChainID, &item.Role, &anchor, &threshold, &version); err != nil {
			unavailable(w)
			return
		}
		item.MessageID = hexValue(message)
		if episode.Valid {
			item.Episode = &episode.Int64
		}
		if resolved.Valid {
			item.ResolvedAt = &resolved.Time
		}
		if reason.Valid {
			item.ResolutionReason = reason.String
		}
		if anchor.Valid {
			item.SLAAnchor = &anchor.Time
		}
		if threshold.Valid {
			item.SLAThresholdMS = &threshold.Int64
		}
		if version.Valid {
			item.PolicyVersion = &version.Int64
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		unavailable(w)
		return
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	next := ""
	if hasMore {
		next = encodeCursor(items[len(items)-1].EventID)
	}
	jsonResponse(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}
