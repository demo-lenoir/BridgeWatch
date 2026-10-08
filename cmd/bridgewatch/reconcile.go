package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"bridgewatch/internal/clock"
	"bridgewatch/internal/destination"
	"bridgewatch/internal/domain"
	"bridgewatch/internal/recovery"
	"bridgewatch/internal/source"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sourceRecoveryReader struct{ source.Reader }

func (r sourceRecoveryReader) HeaderByNumber(ctx context.Context, n uint64) (recovery.Header, error) {
	h, err := r.Reader.HeaderByNumber(ctx, n)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r sourceRecoveryReader) HeaderByHash(ctx context.Context, hash domain.Hash) (recovery.Header, error) {
	h, err := r.Reader.HeaderByHash(ctx, hash)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r sourceRecoveryReader) FilterLogs(ctx context.Context, from, to uint64, address common.Address) error {
	_, err := r.Reader.FilterLogs(ctx, from, to, address)
	return err
}

type destinationRecoveryReader struct{ destination.Reader }

func (r destinationRecoveryReader) HeaderByNumber(ctx context.Context, n uint64) (recovery.Header, error) {
	h, err := r.Reader.HeaderByNumber(ctx, n)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r destinationRecoveryReader) HeaderByHash(ctx context.Context, hash domain.Hash) (recovery.Header, error) {
	h, err := r.Reader.HeaderByHash(ctx, hash)
	return recovery.Header{Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash}, err
}
func (r destinationRecoveryReader) FilterLogs(ctx context.Context, from, to uint64, address common.Address) error {
	_, err := r.Reader.FilterLogs(ctx, from, to, address)
	return err
}

func parseRecoveryHash(value string) (domain.Hash, error) {
	var hash domain.Hash
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return hash, errors.New("expected 32-byte 0x-prefixed hash")
	}
	raw, err := hex.DecodeString(value[2:])
	if err != nil {
		return hash, err
	}
	copy(hash[:], raw)
	return hash, nil
}

func reconcileChain(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("reconcile-chain", flag.ContinueOnError)
	role := flags.String("role", "", "SOURCE or DESTINATION")
	endpointID := flags.String("endpoint-id", "", "trusted configured endpoint identity")
	ancestor := flags.Uint64("ancestor-number", 0, "trusted canonical ancestor number")
	ancestorHash := flags.String("ancestor-hash", "", "trusted canonical ancestor hash")
	checkpoint := flags.Uint64("expected-checkpoint-number", 0, "current persisted checkpoint number")
	checkpointHash := flags.String("expected-checkpoint-hash", "", "current persisted checkpoint hash")
	execute := flags.Bool("execute", false, "apply the inspected plan; default is dry run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*role != "SOURCE" && *role != "DESTINATION") || *endpointID == "" {
		return errors.New("role and endpoint-id are required; no positional arguments are accepted")
	}
	ancestorDigest, err := parseRecoveryHash(*ancestorHash)
	if err != nil {
		return fmt.Errorf("ancestor-hash: %w", err)
	}
	checkpointDigest, err := parseRecoveryHash(*checkpointHash)
	if err != nil {
		return fmt.Errorf("expected-checkpoint-hash: %w", err)
	}
	c, err := readConfig()
	if err != nil {
		return err
	}
	request := recovery.Request{Role: *role, ExpectedCheckpoint: *checkpoint, ExpectedCheckpointHash: checkpointDigest, Ancestor: *ancestor, AncestorHash: ancestorDigest}
	var endpoints []endpointConfig
	if *role == "SOURCE" {
		request.ChainID, request.Contract, request.Genesis = c.sourceID, c.sourceContract, c.sourceGenesis
		request.StartBlock, request.Confirmations = c.sourceStart, c.sourceConfirmations
		endpoints = c.sourceRPCs
	} else {
		request.ChainID, request.Contract, request.Genesis = c.destinationID, c.destinationContract, c.destinationGenesis
		request.StartBlock, request.Confirmations = c.destinationStart, c.destinationConfirmations
		endpoints = c.destinationRPCs
	}
	request.MaxReorgDepth = c.reorgDepth
	if err := request.Validate(); err != nil {
		return err
	}
	var endpointURL string
	for _, endpoint := range endpoints {
		if endpoint.id == *endpointID {
			endpointURL = endpoint.url
			break
		}
	}
	if endpointURL == "" {
		return errors.New("endpoint-id is not configured for selected chain")
	}
	db, err := pgxpool.New(ctx, c.database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return err
	}
	var reader recovery.Reader
	if *role == "SOURCE" {
		r, err := source.DialHTTP(ctx, endpointURL)
		if err != nil {
			return fmt.Errorf("source endpoint %s: %w", *endpointID, err)
		}
		defer r.Close()
		reader = sourceRecoveryReader{r}
	} else {
		r, err := destination.DialHTTP(ctx, endpointURL)
		if err != nil {
			return fmt.Errorf("destination endpoint %s: %w", *endpointID, err)
		}
		defer r.Close()
		reader = destinationRecoveryReader{r}
	}
	service, err := recovery.New(db, reader, clock.Real{})
	if err != nil {
		return err
	}
	var plan recovery.Plan
	if *execute {
		plan, err = service.Execute(ctx, request)
	} else {
		plan, err = service.Plan(ctx, request)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(plan)
}
