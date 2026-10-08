package source

import (
	"context"
	"math/big"
	"testing"

	"bridgewatch/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
)

func TestSourceMetricsBoundedLabels(t *testing.T) {
	chain, _ := domain.NewChainID(big.NewInt(31337))
	if _, err := NewMetrics(prometheus.NewRegistry(), chain, "http://example.invalid/private"); err == nil {
		t.Fatal("endpoint URL accepted as metric label")
	}
	registry := prometheus.NewRegistry()
	metrics, err := NewMetrics(registry, chain, "source_local")
	if err != nil {
		t.Fatal(err)
	}
	reader := WithMetrics(newForkReader(chain), metrics)
	if _, err = reader.ChainID(context.Background()); err != nil {
		t.Fatal(err)
	}
	metrics.SetHead(10)
	metrics.SetLag(2)
	metrics.IncReorg()
	metrics.ObserveState("mockbridge-v1", domain.SourceFinal, nil, "31337_31338")
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 6 {
		t.Fatalf("metric families=%d", len(families))
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "message_id" || label.GetName() == "tx_hash" || label.GetName() == "block_hash" || label.GetName() == "address" {
					t.Fatal("unbounded label")
				}
			}
		}
	}
}
