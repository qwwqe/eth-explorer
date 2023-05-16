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
	// #1
	header, err := getLatestHeader(client)
	if err != nil {
		return err
	}

	fmt.Printf("Latest header: #%v (%v)\n", header.Number, header.Hash())

	// #2
	// lastFetchedBlockNumber := getLastFetchedBlockNumber()
	lastFetchedBlockNumber, err := repo.LastFetchedBlockNumber()
	if err != nil {
		return err
	}
	if lastFetchedBlockNumber == nil {
		lastFetchedBlockNumber = new(big.Int).Sub(header.Number, big.NewInt(BatchSize))
	}

	fmt.Printf("Last fetched: #%v\n", lastFetchedBlockNumber)

	// #3
	mostRecentGapLeft, mostRecentGapRight := getMostRecentGap()
	if mostRecentGapLeft == nil {
		mostRecentGapLeft = big.NewInt(-1)
	}
	if mostRecentGapRight == nil {
		mostRecentGapRight = big.NewInt(0)
	}

	fmt.Printf("Gap left: #%v\n", mostRecentGapLeft)
	fmt.Printf("Gap right: #%v\n", mostRecentGapRight)

	// #4-#8
	p := make([]*big.Int, 0, BatchSize)

	latestDiff := new(big.Int).Sub(header.Number, lastFetchedBlockNumber).Int64()
	for i := int64(1); i <= latestDiff && len(p) < BatchSize; i++ {
		p = append(p, new(big.Int).Sub(header.Number, big.NewInt(i-1)))
	}

	newBlocks := len(p)
	fmt.Printf("Fetching %v new blocks\n", len(p))

	gapDiff := new(big.Int).Sub(mostRecentGapRight, mostRecentGapLeft).Int64() - 1
	for i := int64(1); i <= gapDiff && len(p) < BatchSize; i++ {
		p = append(p, new(big.Int).Sub(mostRecentGapRight, big.NewInt(i)))
	}

	fmt.Printf("Fetching %v old blocks\n", len(p)-newBlocks)

	blockErrors := make(chan error)
	blockResponses := make(chan *types.Block)

	for _, n := range p {
		n := n
		go func() {
			block, err := client.BlockByNumber(context.TODO(), n)

			if err != nil {
				blockErrors <- err
			} else {
				blockResponses <- block
			}
		}()
	}

	pending := len(p)

	for pending > 0 {
		select {
		case block := <-blockResponses:
			fetchedBlocks.mut.Lock()
			fetchedBlocks.Add(block.Number())
			fetchedBlocks.mut.Unlock()

			pending--
			fmt.Printf("Received block: %v (%v)\n", block.Number(), block.Hash())
		case err := <-blockErrors:
			return err
		}
	}

	return nil
}

func getLatestHeader(client *ethclient.Client) (*types.Header, error) {
	return client.HeaderByNumber(context.TODO(), nil)
}

func getMostRecentGap() (*big.Int, *big.Int) {
	fetchedBlocks.mut.RLock()
	defer fetchedBlocks.mut.RUnlock()

	if len(fetchedBlocks.numbers) == 0 {
		return nil, nil
	}

	if len(fetchedBlocks.numbers) == 1 {
		return nil, fetchedBlocks.numbers[0]
	}

	var l, r *big.Int

	sum := new(big.Int)
	one := big.NewInt(1)
	for i := len(fetchedBlocks.numbers) - 2; i >= 0 && r == nil; i-- {
		if sum.Add(fetchedBlocks.numbers[i], one).Cmp(fetchedBlocks.numbers[i+1]) != 0 {
			l = fetchedBlocks.numbers[i]
			r = fetchedBlocks.numbers[i+1]
		}
	}

	if r == nil {
		return nil, fetchedBlocks.numbers[0]
	}

	return l, r
}
