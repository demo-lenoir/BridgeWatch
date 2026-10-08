package destination

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"bridgewatch/internal/operations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func benchmarkHistogram(t *testing.T, db *pgxpool.Pool, query string) map[string]int {
	t.Helper()
	rows, err := db.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := map[string]int{}
	for rows.Next() {
		var label string
		var count int
		if err := rows.Scan(&label, &count); err != nil {
			t.Fatal(err)
		}
		values[label] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

type benchmarkEvidence struct {
	Valid                   bool               `json:"valid"`
	Workload                map[string]any     `json:"workload"`
	Environment             map[string]any     `json:"environment"`
	Counts                  map[string]int     `json:"counts"`
	StateHistogram          map[string]int     `json:"state_histogram"`
	AnomalyHistogram        map[string]int     `json:"anomaly_histogram"`
	PayloadChecksum         string             `json:"payload_checksum"`
	ExpectedPayloadChecksum string             `json:"expected_payload_checksum"`
	Measurements            map[string]float64 `json:"measurements"`
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	index := int(float64(len(values)-1)*quantile + 0.5)
	return values[index]
}

func cpuSeconds() float64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func TestPhase5BenchmarkWorkload(t *testing.T) {
	path := os.Getenv("BRIDGEWATCH_BENCH_EVIDENCE")
	if path == "" {
		t.Skip("benchmark evidence path not configured")
	}
	count := 80
	if value := os.Getenv("BRIDGEWATCH_BENCH_MESSAGES"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 20 && count != 80 {
		t.Fatal("benchmark message count must be 20 or 80")
	}
	started := time.Now()
	cpuStart := cpuSeconds()
	var memoryStart runtime.MemStats
	runtime.ReadMemStats(&memoryStart)
	r := newCrossRig(t)
	ctx := context.Background()
	clock := &controlledClock{now: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)}
	r.sourceRepo.SetClock(clock)
	r.destRepo.SetClock(clock)
	r.syncDest(t)
	service, err := operations.NewService(r.db, clock, r.sourceWatcher, r.destWatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ConfigurePolicy(ctx, operations.Policy{Protocol: "mockbridge-v1", SourceChainID: r.sourceID, DestinationChainID: r.destID, Version: 1, SLA: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	sourceBranch := extend(r.sourceChain.branch, 10, count+3)
	for i := 1; i <= count; i++ {
		r.addSource(t, sourceBranch[i+1], byte(i), byte(i+20))
	}
	r.sourceChain.setBranch(sourceBranch)
	r.syncSource(t)
	sourceObserved := time.Now()
	for {
		n, err := service.ActivateEligible(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	correlationLatencies := make([]float64, 0, count)
	completionLatencies := make([]float64, 0, count)
	branch := r.destChain.branch
	for first := 1; first <= count; first += 10 {
		for i := first; i < first+10; i++ {
			branch = extend(branch, 30, len(branch))
			if i%10 != 0 {
				r.addDestination(t, branch[len(branch)-1], byte(i), byte(i+50), 7, 4)
				if i%20 == 1 {
					r.addDestination(t, branch[len(branch)-1], byte(i), byte(i+80), 7, 4)
				}
			}
		}
		r.destChain.setBranch(branch)
		r.syncDest(t)
		seenMS := float64(time.Since(sourceObserved).Microseconds()) / 1000
		branch = extend(branch, 30, len(branch)+1)
		r.destChain.setBranch(branch)
		r.syncDest(t)
		completedMS := float64(time.Since(sourceObserved).Microseconds()) / 1000
		for i := first; i < first+10; i++ {
			if i%10 != 0 && i%20 != 1 {
				correlationLatencies = append(correlationLatencies, seenMS)
				completionLatencies = append(completionLatencies, completedMS)
			}
		}
	}
	clock.Set(clock.Now().Add(30 * time.Second))
	stuckCount, err := service.ClassifyDue(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if stuckCount != count/10 {
		t.Fatal("unexpected stuck count", stuckCount)
	}
	duration := time.Since(started).Seconds()
	var memoryEnd runtime.MemStats
	runtime.ReadMemStats(&memoryEnd)
	counts := map[string]int{}
	queries := map[string]string{"messages": `SELECT count(*) FROM cross_chain_messages`, "transitions": `SELECT count(*) FROM message_transitions`, "observations": `SELECT count(*) FROM message_observations`, "anomalies": `SELECT count(*) FROM message_anomalies`, "alert_episodes": `SELECT count(*) FROM message_alerts`, "duplicate_business_transitions": `SELECT count(*) FROM (SELECT to_state,lag(to_state) OVER (PARTITION BY message_pk ORDER BY revision) AS preceding FROM message_transitions) x WHERE to_state=preceding`}
	for name, query := range queries {
		var n int
		if err := r.db.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[name] = n
	}
	states := benchmarkHistogram(t, r.db, `SELECT state,count(*) FROM cross_chain_messages GROUP BY state`)
	anomalies := benchmarkHistogram(t, r.db, `SELECT kind,count(*) FROM message_anomalies GROUP BY kind`)
	rows, err := r.db.Query(ctx, `SELECT message_id,payload_hash FROM cross_chain_messages ORDER BY message_id`)
	if err != nil {
		t.Fatal(err)
	}
	actualDigest := sha256.New()
	for rows.Next() {
		var id, payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		actualDigest.Write(id)
		actualDigest.Write(payload)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	expectedDigest := sha256.New()
	for i := 1; i <= count; i++ {
		id := make([]byte, 32)
		id[0] = byte(i)
		expectedDigest.Write(id)
		payload := make([]byte, 32)
		payload[0] = 7
		expectedDigest.Write(payload)
	}
	actualChecksum := hex.EncodeToString(actualDigest.Sum(nil))
	expectedChecksum := hex.EncodeToString(expectedDigest.Sum(nil))
	var postgresVersion string
	if err := r.db.QueryRow(ctx, `SHOW server_version`).Scan(&postgresVersion); err != nil {
		t.Fatal(err)
	}
	dup := count / 20
	delayed := count / 10
	expectedCompleted := count - dup - delayed
	expectedTransitions := count * 69 / 20
	if count == 20 {
		expectedTransitions = 68
	}
	valid := counts["messages"] == count && counts["observations"] == count*39/20 && counts["transitions"] == expectedTransitions && counts["anomalies"] == dup+delayed && counts["alert_episodes"] == delayed && counts["duplicate_business_transitions"] == 0 && actualChecksum == expectedChecksum && len(states) == 3 && states["COMPLETED"] == expectedCompleted && states["RELAY_PENDING"] == delayed && states["SOURCE_SEEN"] == dup && len(anomalies) == 2 && anomalies["DUPLICATE_EXECUTION"] == dup && anomalies["STUCK"] == delayed
	evidence := benchmarkEvidence{Valid: valid, Workload: map[string]any{"source_messages": count, "destination_executions": count - delayed + dup, "duplicate_rate": float64(dup) / float64(count), "conflicting_duplicate_rate": 0, "delayed_stuck_ratio": float64(delayed) / float64(count), "source_watcher_workers": 1, "destination_watcher_workers": 1, "range_size_blocks": 8, "db_pool_max_connections": r.db.Config().MaxConns, "rpc_latency_fixture": "in-process zero delay", "rpc_failure_fixture": "none", "source_confirmations": 3, "destination_confirmations": 3}, Environment: map[string]any{"os": runtime.GOOS, "architecture": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "go": runtime.Version(), "postgresql": postgresVersion, "anvil": os.Getenv("BRIDGEWATCH_ANVIL_VERSION"), "host_cpu": os.Getenv("BRIDGEWATCH_HOST_CPU"), "host_memory_bytes": os.Getenv("BRIDGEWATCH_HOST_MEMORY_BYTES")}, Counts: counts, StateHistogram: states, AnomalyHistogram: anomalies, PayloadChecksum: actualChecksum, ExpectedPayloadChecksum: expectedChecksum, Measurements: map[string]float64{"messages_per_second": float64(count) / duration, "source_observation_to_correlation_p50_ms": percentile(correlationLatencies, 0.50), "source_observation_to_correlation_p95_ms": percentile(correlationLatencies, 0.95), "completion_classification_p50_ms": percentile(completionLatencies, 0.50), "completion_classification_p95_ms": percentile(completionLatencies, 0.95), "rows_per_second": float64(counts["messages"]+counts["observations"]+counts["transitions"]) / duration, "cpu_seconds": cpuSeconds() - cpuStart, "heap_alloc_delta_bytes": float64(int64(memoryEnd.Alloc) - int64(memoryStart.Alloc)), "memory_sys_bytes": float64(memoryEnd.Sys), "duration_seconds": duration}}
	raw, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("benchmark valid=%t messages=%d states=%v anomalies=%v checksum=%s", valid, count, states, anomalies, actualChecksum)
	if !valid {
		t.Fatal("benchmark correctness gate failed: " + strings.TrimSpace(fmt.Sprint(counts)))
	}
}
