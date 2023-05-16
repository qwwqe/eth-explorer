package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/time/rate"
)

type BlockFetcher struct {
	client  *ethclient.Client
	repo    *BlockRepo
	config  *Config
	limiter *rate.Limiter
}

func NewBlockFetcher(client *ethclient.Client, repo *BlockRepo, config *Config) (*BlockFetcher, error) {
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

	fmt.Printf("Latest header: #%v (%v)\n", header.Number, header.Hash())

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

	blockErrors := make(chan error)
	blockCompletions := make(chan struct{})

	blockResponses := new([]*types.Block)
	blockResponseMut := sync.Mutex{}

	for _, n := range p {
		n := n
		go func() {
			f.limiter.WaitN(context.TODO(), 2)
			block, err := f.client.BlockByNumber(context.TODO(), n)

			if err != nil {
				blockErrors <- err
			} else {
				fmt.Printf("Received block: %v (%v)\n", block.Number(), block.Hash())

				blockResponseMut.Lock()
				*blockResponses = append(*blockResponses, block)
				blockResponseMut.Unlock()

				blockCompletions <- struct{}{}
			}
		}()
	}

	pending := len(p)

	for pending > 0 {
		select {
		case <-blockCompletions:
			pending--
		case err := <-blockErrors:
			return err
		}
	}

	if err := f.repo.SaveBlocks(*blockResponses); err != nil {
		return err
	}

	return nil
}

func (f *BlockFetcher) GetLatestHeader() (*types.Header, error) {
	return f.client.HeaderByNumber(context.TODO(), nil)
}
