package destination

import (
	"math/big"
	"testing"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/source"
	"github.com/prometheus/client_golang/prometheus"
)

func TestDestinationMetricsBoundedLabelsAndSharedRegistry(t *testing.T) {
	sourceID, _ := domain.NewChainID(big.NewInt(31337))
	destID, _ := domain.NewChainID(big.NewInt(31338))
	registry := prometheus.NewRegistry()
	if _, err := source.NewMetrics(registry, sourceID, "source_local"); err != nil {
		t.Fatal(err)
	}
	metrics, err := NewMetrics(registry, destID, "destination_local")
	if err != nil {
		t.Fatal(err)
	}
	metrics.SetHead(12)
	metrics.SetLag(2)
	metrics.IncReorg()
	metrics.ObserveState("mockbridge-v1", domain.UnmatchedDestination, nil, "31337_31338")
	seconds := 1.5
	metrics.ObserveState("mockbridge-v1", domain.Completed, &seconds, "31337_31338")
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"bridgewatch_source_head_block": false, "bridgewatch_destination_head_block": false, "bridgewatch_lag_blocks": false, "bridgewatch_reorgs_total": false, "bridgewatch_messages_total": false, "bridgewatch_delivery_seconds": false, "bridgewatch_unmatched_destination_total": false}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "message_id" || label.GetName() == "tx_hash" || label.GetName() == "address" || label.GetName() == "block_hash" {
					t.Fatalf("unbounded metric label: %s", label.GetName())
				}
			}
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("missing metric %s", name)
		}
	}
	if _, err = NewMetrics(prometheus.NewRegistry(), destID, "https://rpc.example"); err == nil {
		t.Fatal("URL accepted as endpoint label")
	}
}
