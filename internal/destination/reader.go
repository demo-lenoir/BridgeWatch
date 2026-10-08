package destination

import (
	"context"
	"errors"
	"math/big"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/rpcpool"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Header struct {
	Number     uint64
	Hash       domain.Hash
	ParentHash domain.Hash
}

type Reader interface {
	ChainID(context.Context) (domain.ChainID, error)
	BlockNumber(context.Context) (uint64, error)
	HeaderByNumber(context.Context, uint64) (Header, error)
	HeaderByHash(context.Context, domain.Hash) (Header, error)
	FilterLogs(context.Context, uint64, uint64, common.Address) ([]types.Log, error)
}

type HTTPReader struct{ client *ethclient.Client }

func DialHTTP(ctx context.Context, url string) (*HTTPReader, error) {
	client, err := ethclient.DialContext(ctx, url)
	if err != nil {
		return nil, rpcpool.PublicError(err)
	}
	return &HTTPReader{client: client}, nil
}

func (r *HTTPReader) Close() { r.client.Close() }

func (r *HTTPReader) ChainID(ctx context.Context) (domain.ChainID, error) {
	id, err := r.client.ChainID(ctx)
	if err != nil {
		return domain.ChainID{}, rpcpool.PublicError(err)
	}
	return domain.NewChainID(id)
}

func (r *HTTPReader) BlockNumber(ctx context.Context) (uint64, error) {
	head, err := r.client.BlockNumber(ctx)
	return head, rpcpool.PublicError(err)
}

func toHeader(header *types.Header) (Header, error) {
	if header == nil || header.Number == nil || header.Number.Sign() < 0 || !header.Number.IsUint64() {
		return Header{}, errors.New("malformed destination header number")
	}
	return Header{Number: header.Number.Uint64(), Hash: domain.Hash(header.Hash()), ParentHash: domain.Hash(header.ParentHash)}, nil
}

func (r *HTTPReader) HeaderByNumber(ctx context.Context, number uint64) (Header, error) {
	header, err := r.client.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		return Header{}, rpcpool.PublicError(err)
	}
	return toHeader(header)
}

func (r *HTTPReader) HeaderByHash(ctx context.Context, hash domain.Hash) (Header, error) {
	header, err := r.client.HeaderByHash(ctx, common.Hash(hash))
	if err != nil {
		return Header{}, rpcpool.PublicError(err)
	}
	return toHeader(header)
}

func (r *HTTPReader) FilterLogs(ctx context.Context, from, to uint64, address common.Address) ([]types.Log, error) {
	if from > to {
		return nil, errors.New("invalid destination log range")
	}
	logs, err := r.client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: new(big.Int).SetUint64(from), ToBlock: new(big.Int).SetUint64(to), Addresses: []common.Address{address}})
	return logs, rpcpool.PublicError(err)
}
