package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

type Transaction struct {
	Hash        common.Hash        `json:"hash"`
	FromAddress string             `json:"from"`
	ToAddress   string             `json:"to"`
	Nonce       *big.Int           `json:"nonce"`
	Value       *big.Int           `json:"value"`
	Input       string             `json:"data"`
	Logs        []*json.RawMessage `json:"logs"`
}

func (t *Transaction) UnmarshalJSON(b []byte) error {
	type transaction struct {
		Hash        common.Hash      `json:"hash"`
		FromAddress string           `json:"from"`
		ToAddress   string           `json:"to"`
		Nonce       *json.RawMessage `json:"nonce"`
		Value       *json.RawMessage `json:"value"`
		Input       string           `json:"data"`
	}

	var tx transaction
	if err := json.Unmarshal(b, &tx); err != nil {
		return err
	}

	t.Hash = tx.Hash
	t.FromAddress = tx.FromAddress
	t.ToAddress = tx.ToAddress
	t.Input = tx.Input

	if tx.Nonce != nil && string(*tx.Nonce) != "null" {
		s := strings.Trim(string(*tx.Nonce), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		t.Nonce = i
	}

	if tx.Value != nil && string(*tx.Value) != "null" {
		s := strings.Trim(string(*tx.Value), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		t.Value = i
	}

	return nil
}

type TransactionReceipt struct {
	TransactionHash common.Hash        `json:"transactionHash"`
	Logs            []*json.RawMessage `json:"logs"`
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

	limiter := rate.NewLimiter(fetchRate, int(math.Max(float64(config.TxBatchSize), float64(config.HeaderBatchSize))))

	return &BlockFetcher{client, repo, config, limiter}, nil
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
		newestFetchedBlockNumber = new(big.Int).Sub(header.Number, big.NewInt(int64(f.config.HeaderBatchSize)))
	}

	oldestFetchedBlockNumber, err := f.repo.OldestFetchedBlockNumber()
	if err != nil {
		return err
	}

	fmt.Printf("Last fetched: #%v\n", newestFetchedBlockNumber)

	p := make([]*big.Int, 0, f.config.HeaderBatchSize)

	newBlocks := new(big.Int).Sub(header.Number, newestFetchedBlockNumber).Int64()
	for i := int64(1); i <= newBlocks && len(p) < f.config.HeaderBatchSize; i++ {
		p = append(p, new(big.Int).Add(newestFetchedBlockNumber, big.NewInt(i)))
	}

	oldBlocks := int64(f.config.HeaderBatchSize - len(p))
	if oldestFetchedBlockNumber != nil && len(p) < f.config.HeaderBatchSize {
		for i := int64(1); i <= oldBlocks && len(p) < f.config.HeaderBatchSize; i++ {
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

	fmt.Printf("Retrieved %v block headers (%v new, %v old)\n", len(blockHeaders), newBlocks, oldBlocks)

	transactions, err := f.FetchTransactions(blockHeaders)
	if err != nil {
		return err
	}

	fmt.Printf("Retrieved %v transactions\n", len(transactions))

	if err := f.PopulateTransactionLogs(transactions); err != nil {
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

func (f *BlockFetcher) FetchTransactions(headers []*BlockHeader) ([]*Transaction, error) {
	transactionHashes := []string{}

	for _, h := range headers {
		transactionHashes = append(transactionHashes, h.TransactionHashes...)
	}

	transactions := make([]*Transaction, 0, len(transactionHashes))
	transactionResults := make(chan []*Transaction)
	transactionErrors := make(chan error)
	pending := 0

	for i := 0; i < len(transactionHashes); i += f.config.TxBatchSize {
		l, r := i, int(math.Min(float64(len(transactionHashes)-1), float64(i+f.config.TxBatchSize)))
		pending++
		go func() {
			f.limiter.Wait(context.TODO())
			txs, err := f.GetTransactionsByHash(transactionHashes[l:r])
			if err != nil {
				transactionErrors <- err
				return
			}

			transactionResults <- txs
		}()
	}

	for pending > 0 {
		select {
		case txs := <-transactionResults:
			transactions = append(transactions, txs...)
			pending--
		case err := <-transactionErrors:
			return nil, err
		}
	}

	return transactions, nil
}

func (f *BlockFetcher) PopulateTransactionLogs(transactions []*Transaction) error {
	receiptResults := make(chan []*TransactionReceipt)
	receiptErrors := make(chan error)
	pending := 0

	lookup := map[string]*Transaction{}

	for _, t := range transactions {
		lookup[t.Hash.Hex()] = t
	}

	for i := 0; i < len(transactions); i += f.config.TxBatchSize {
		l, r := i, int(math.Min(float64(len(transactions)-1), float64(i+f.config.TxBatchSize)))
		pending++
		go func() {
			f.limiter.Wait(context.TODO())
			rs, err := f.GetTransactionReceipts(transactions[l:r])
			if err != nil {
				receiptErrors <- err
				return
			}

			receiptResults <- rs
		}()
	}

	for pending > 0 {
		select {
		case rs := <-receiptResults:
			for _, r := range rs {
				if t, ok := lookup[r.TransactionHash.Hex()]; ok {
					t.Logs = r.Logs
				} else {
					return fmt.Errorf("Could not find corresponding transaction %v for retrieved logs", t.Hash.Hex())
				}
			}
			pending--
		case err := <-receiptErrors:
			return err
		}
	}

	return nil
}

func (f *BlockFetcher) FetchAll() error {
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

func (f *BlockFetcher) GetTransactionsByHash(hashes []string) ([]*Transaction, error) {
	methods := make([]rpc.BatchElem, len(hashes))
	results := make([]*Transaction, len(hashes))

	for i, h := range hashes {
		methods[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{h},
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

func (f *BlockFetcher) GetTransactionReceipts(transactions []*Transaction) ([]*TransactionReceipt, error) {
	methods := make([]rpc.BatchElem, len(transactions))
	results := make([]*TransactionReceipt, len(transactions))

	for i, tx := range transactions {
		methods[i] = rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []interface{}{tx.Hash},
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
