package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/time/rate"
)

type BlockHeader struct {
	Number            *big.Int    `json:"number"`
	ParentHash        common.Hash `json:"parentHash"`
	Hash              common.Hash `json:"hash"`
	Time              uint64      `json:"timestamp"`
	TransactionHashes []string    `json:"transactions"`
}

func (h *BlockHeader) UnmarshalJSON(b []byte) error {
	type blockHeader struct {
		Number            *json.RawMessage `json:"number"`
		ParentHash        common.Hash      `json:"parentHash"`
		Hash              common.Hash      `json:"hash"`
		Time              *json.RawMessage `json:"timestamp"`
		TransactionHashes []string         `json:"transactions"`
	}

	var bh blockHeader
	if err := json.Unmarshal(b, &bh); err != nil {
		return err
	}

	h.ParentHash = bh.ParentHash
	h.Hash = bh.Hash
	h.TransactionHashes = bh.TransactionHashes

	if bh.Number != nil && string(*bh.Number) != "null" {
		s := strings.Trim(string(*bh.Number), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		h.Number = i
	}

	if bh.Time != nil && string(*bh.Time) != "null" {
		s := strings.Trim(string(*bh.Time), `"`)

		t, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		h.Time = t.Uint64()
	}

	return nil
}

type BlockFetcher struct {
	client  *rpc.Client
	repo    *BlockRepo
	config  *Config
	limiter *rate.Limiter
}

func NewBlockFetcher(client *rpc.Client, repo *BlockRepo, config *Config) (*BlockFetcher, error) {
	var fetchRate rate.Limit

	if config.RateLimitSeconds.Seconds() <= 0 || config.RateLimitValue <= 0 {
		fetchRate = rate.Inf
	} else {
		fetchRate = rate.Limit(config.RateLimitValue / int(config.RateLimitSeconds.Seconds()))
	}

	if fetchRate == 0 && (config.RateLimitValue != 0 || config.RateLimitSeconds.Seconds() != 0) {
		return nil, errors.New(fmt.Sprintf("Fetching rate limit cannot be less than one event per second"))
	}

	return &BlockFetcher{client, repo, config, rate.NewLimiter(fetchRate, config.BatchSize)}, nil
}

func (f *BlockFetcher) FetchBlocks() error {
	if err := f.limiter.Wait(context.TODO()); err != nil {
		return err
	}

	header, err := f.GetLatestHeader()
	if err != nil {
		return err
	}

	fmt.Printf("Latest header: #%v (%v)\n", header.Number, header.Hash)

	newestFetchedBlockNumber, err := f.repo.NewestFetchedBlockNumber()
	if err != nil {
		return err
	} else if newestFetchedBlockNumber == nil {
		newestFetchedBlockNumber = new(big.Int).Sub(header.Number, big.NewInt(int64(f.config.BatchSize)))
	}

	oldestFetchedBlockNumber, err := f.repo.OldestFetchedBlockNumber()
	if err != nil {
		return err
	}

	fmt.Printf("Last fetched: #%v\n", newestFetchedBlockNumber)

	p := make([]*big.Int, 0, f.config.BatchSize)

	newBlocks := new(big.Int).Sub(header.Number, newestFetchedBlockNumber).Int64()
	for i := int64(1); i <= newBlocks && len(p) < f.config.BatchSize; i++ {
		p = append(p, new(big.Int).Add(newestFetchedBlockNumber, big.NewInt(i)))
	}

	fmt.Printf("Fetching %v new blocks\n", len(p))

	if oldestFetchedBlockNumber != nil && len(p) < f.config.BatchSize {
		oldBlocks := int64(f.config.BatchSize - len(p))
		fmt.Printf("Fetching %v old blocks\n", oldBlocks)
		for i := int64(1); i <= oldBlocks && len(p) < f.config.BatchSize; i++ {
			// todo: deal with negative block numbers
			p = append(p, new(big.Int).Sub(oldestFetchedBlockNumber, big.NewInt(i)))
		}
	}

	// 幣安的rate limit好像是針對HTTP請求而言（無論payload多少rpc method）
	f.limiter.Wait(context.TODO())

	blockHeaders, err := f.GetHeadersByNumber(p)
	if err != nil {
		return err
	}

	// blockErrors := make(chan error)
	// blockCompletions := make(chan struct{})

	// blockResponses := new([]*types.Block)
	// blockResponseMut := sync.Mutex{}

	// for _, n := range p {
	// 	n := n
	// 	go func() {
	// 		f.limiter.WaitN(context.TODO(), 2)
	// 		block, err := ethclient.NewClient(f.client).BlockByNumber(context.TODO(), n)

	// 		if err != nil {
	// 			blockErrors <- err
	// 			return
	// 		}
	// 		fmt.Printf("Received block: %v (%v)\n", block.Number(), block.Hash())

	// 		blockResponseMut.Lock()
	// 		*blockResponses = append(*blockResponses, block)
	// 		blockResponseMut.Unlock()

	// 		blockCompletions <- struct{}{}
	// 	}()
	// }

	// pending := len(p)

	// for pending > 0 {
	// 	select {
	// 	case <-blockCompletions:
	// 		pending--
	// 	case err := <-blockErrors:
	// 		return err
	// 	}
	// }

	if err := f.repo.SaveBlocks(blockHeaders); err != nil {
		return err
	}

	return nil
}

func (f *BlockFetcher) GetLatestHeader() (*BlockHeader, error) {
	var header *BlockHeader
	err := f.client.CallContext(context.TODO(), &header, "eth_getBlockByNumber", "latest", false)
	if err == nil && header == nil {
		return nil, ethereum.NotFound
	}
	return header, err
}

func (f *BlockFetcher) GetHeadersByNumber(numbers []*big.Int) ([]*BlockHeader, error) {
	methods := make([]rpc.BatchElem, len(numbers))
	results := make([]*BlockHeader, len(numbers))

	for i, n := range numbers {
		methods[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{fmt.Sprint("0x", n.Text(16)), false},
			Result: &results[i],
		}
	}

	if err := f.client.BatchCall(methods); err != nil {
		return nil, err
	}

	for i, elem := range methods {
		if elem.Error != nil {
			return nil, elem.Error
		}
		if results[i] == nil {
			return nil, fmt.Errorf("Received null header for block %v", elem.Args[0])
		}
	}

	return results, nil
}

func (f *BlockFetcher) Fetch() error {
	fetcherErrors := make(chan error)

	go func() {
		for {
			if err := f.FetchBlocks(); err != nil {
				fetcherErrors <- err
			}
			fmt.Printf("Tokens remaining: %v\n", f.limiter.Tokens())
		}
	}()

	if err := <-fetcherErrors; err != nil {
		return err
	}

	return nil
}
