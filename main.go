package main

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const BatchSize = 5

var host = "https://data-seed-prebsc-1-s1.binance.org:8545/"

type BlockNumbers struct {
	numbers []*big.Int
	mut     sync.RWMutex
}

func (b *BlockNumbers) Add(i ...*big.Int) {
	b.numbers = append(b.numbers, i...)
	sort.SliceStable(b.numbers, func(x, y int) bool {
		return b.numbers[x].Cmp(b.numbers[y]) == -1
	})
}

type Config struct {
	DbHost     string `env:"ETHEXPLORER_DB_HOST"`
	DbPort     string `env:"ETHEXPLORER_DB_PORT"`
	DbUser     string `env:"ETHEXPLORER_DB_USER"`
	DbPassword string `env:"ETHEXPLORER_DB_PASSWORD"`
	DbName     string `env:"ETHEXPLORER_DB_NAME"`
	RpcNode    string `env:"ETHEXPLORER_RPC_NODE"`
	BatchSize  int    `env:"ETHEXPLORER_BATCH_SIZE"`
}

var fetchedBlocks BlockNumbers

func main() {
	// todo: 用rpcclient
	client, err := ethclient.Dial(host)
	if err != nil {
		panic(err)
	}

	config := Config{
		DbHost:     "127.0.0.1",
		DbPort:     "3306",
		DbUser:     "eth",
		DbPassword: "eth",
		DbName:     "eth",
		RpcNode:    "https://data-seed-prebsc-1-s1.binance.org:8545/",
		BatchSize:  5,
	}

	repo := &BlockRepo{}

	if err := repo.Open(config); err != nil {
		panic(err)
	}

	reader := bufio.NewReader(os.Stdin)

	for i := 1; true; i++ {
		if err := step(client, repo); err != nil {
			panic(err)
		}

		reader.ReadLine()
	}
}

func step(client *ethclient.Client, repo *BlockRepo) error {
	header, err := getLatestHeader(client)
	if err != nil {
		return err
	}

	fmt.Printf("Latest header: #%v (%v)\n", header.Number, header.Hash())

	newestFetchedBlockNumber, err := repo.NewestFetchedBlockNumber()
	if err != nil {
		return err
	} else if newestFetchedBlockNumber == nil {
		newestFetchedBlockNumber = new(big.Int).Sub(header.Number, big.NewInt(BatchSize))
	}

	oldestFetchedBlockNumber, err := repo.OldestFetchedBlockNumber()
	if err != nil {
		return err
	}

	fmt.Printf("Last fetched: #%v\n", newestFetchedBlockNumber)

	p := make([]*big.Int, 0, BatchSize)

	newBlocks := new(big.Int).Sub(header.Number, newestFetchedBlockNumber).Int64()
	for i := int64(1); i <= newBlocks && len(p) < BatchSize; i++ {
		p = append(p, new(big.Int).Add(newestFetchedBlockNumber, big.NewInt(i)))
	}

	fmt.Printf("Fetching %v new blocks\n", len(p))

	if oldestFetchedBlockNumber != nil && len(p) < BatchSize {
		oldBlocks := int64(BatchSize - len(p))
		fmt.Printf("Fetching %v old blocks\n", oldBlocks)
		for i := int64(1); i <= oldBlocks && len(p) < BatchSize; i++ {
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
			block, err := client.BlockByNumber(context.TODO(), n)

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

	if err := repo.SaveBlocks(*blockResponses); err != nil {
		return err
	}

	return nil
}

func getLatestHeader(client *ethclient.Client) (*types.Header, error) {
	return client.HeaderByNumber(context.TODO(), nil)
}
