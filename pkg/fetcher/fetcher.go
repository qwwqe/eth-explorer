package fetcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/qwwqe/eth-explorer/pkg/common"
	"github.com/qwwqe/eth-explorer/pkg/repo"
	"golang.org/x/time/rate"
)

type BlockFetcher struct {
	client  *rpc.Client
	repo    *repo.BlockRepo
	config  *common.Config
	limiter *rate.Limiter
}

func NewBlockFetcher(client *rpc.Client, repo *repo.BlockRepo, config *common.Config) (*BlockFetcher, error) {
	var fetchRate rate.Limit

	if config.RateLimitSeconds <= 0 || config.RateLimitValue <= 0 {
		fetchRate = rate.Inf
	} else {
		fetchRate = rate.Limit(config.RateLimitValue / int(config.RateLimitSeconds))
	}

	if fetchRate == 0 && (config.RateLimitValue != 0 || config.RateLimitSeconds != 0) {
		return nil, errors.New(fmt.Sprintf("Fetching rate limit cannot be less than one event per second"))
	}

	limiter := rate.NewLimiter(fetchRate, int(math.Max(float64(config.TxBatchSize), float64(config.HeaderBatchSize))))

	return &BlockFetcher{client, repo, config, limiter}, nil
}

func (f *BlockFetcher) FetchBlocks() ([]*common.BlockHeader, error) {
	f.limiter.Wait(context.TODO())

	header, err := f.GetLatestHeader()
	if err != nil {
		return nil, err
	}

	fmt.Printf("Latest header: #%v\n", header.Number)

	newestFetchedBlockNumber, err := f.repo.NewestFetchedBlockNumber()
	if err != nil {
		return nil, err
	} else if newestFetchedBlockNumber == nil {
		newestFetchedBlockNumber = new(big.Int).Sub(header.Number, big.NewInt(int64(f.config.HeaderBatchSize)))
	}

	oldestFetchedBlockNumber, err := f.repo.OldestFetchedBlockNumber()
	if err != nil {
		return nil, err
	}

	fmt.Printf("Last fetched: #%v\n", newestFetchedBlockNumber)

	p := make([]*big.Int, 0, f.config.HeaderBatchSize)

	newBlocks := int64(math.Min(
		float64(f.config.HeaderBatchSize),
		float64(new(big.Int).Sub(header.Number, newestFetchedBlockNumber).Int64()),
	))
	for i := int64(1); i <= newBlocks && len(p) < f.config.HeaderBatchSize; i++ {
		p = append(p, new(big.Int).Add(newestFetchedBlockNumber, big.NewInt(i)))
	}

	oldBlocks := int64(f.config.HeaderBatchSize - len(p))
	bigZero := big.NewInt(0)
	if oldestFetchedBlockNumber != nil && len(p) < f.config.HeaderBatchSize {
		for i := int64(1); i <= oldBlocks && len(p) < f.config.HeaderBatchSize; i++ {
			n := new(big.Int).Sub(oldestFetchedBlockNumber, big.NewInt(i))
			if n.Cmp(bigZero) < 0 {
				break
			}
			p = append(p, n)
		}
	}

	fmt.Printf("Fetching new headers: %v\n", newBlocks)
	fmt.Printf("Fetching old headers: %v\n", oldBlocks)

	// 幣安的rate limit好像是針對HTTP請求而言（無論payload多少rpc method）
	f.limiter.Wait(context.TODO())

	blockHeaders, err := f.GetHeadersByNumber(p)
	if err != nil {
		return nil, err
	}

	return blockHeaders, nil
}

func (f *BlockFetcher) FetchTransactions(headers []*common.BlockHeader) ([]*common.Transaction, error) {
	transactionHashes := []string{}

	for _, h := range headers {
		transactionHashes = append(transactionHashes, h.TransactionHashes...)
	}

	transactions := make([]*common.Transaction, 0, len(transactionHashes))
	transactionResults := make(chan []*common.Transaction)
	transactionErrors := make(chan error)

	pending := 0
	for i := 0; i < len(transactionHashes); i += f.config.TxBatchSize {
		pending++
		l, r := i, int(math.Min(float64(len(transactionHashes)-1), float64(i+f.config.TxBatchSize)))

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

func (f *BlockFetcher) PopulateTransactionLogs(transactions []*common.Transaction) error {
	receiptResults := make(chan []*common.TransactionReceipt)
	receiptErrors := make(chan error)

	lookup := map[string]*common.Transaction{}
	for _, t := range transactions {
		lookup[t.Hash.Hex()] = t
	}

	pending := 0
	for i := 0; i < len(transactions); i += f.config.LogBatchSize {
		pending++
		l, r := i, int(math.Min(float64(len(transactions)-1), float64(i+f.config.LogBatchSize)))

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
			return fmt.Errorf("Error processing receipt: %v", err)
		}
	}

	return nil
}

func (f *BlockFetcher) FetchAll() error {
	blockHeaders, err := f.FetchBlocks()
	if err != nil {
		return err
	}

	fmt.Printf("Retrieved %v block headers\n", len(blockHeaders))

	transactions, err := f.FetchTransactions(blockHeaders)
	if err != nil {
		return err
	}

	fmt.Printf("Retrieved %v transactions\n", len(transactions))

	if err := f.PopulateTransactionLogs(transactions); err != nil {
		return err
	}

	tx, err := f.repo.BeginTx(context.TODO())
	if err != nil {
		return err
	}

	if err := f.repo.SaveBlocksTx(tx, blockHeaders); err != nil {
		return err
	}

	if err := f.repo.SaveTransactionsTx(tx, transactions); err != nil {
		return err
	}

	if err := f.repo.CommitTx(tx); err != nil {
		return err
	}

	return nil
}

func (f *BlockFetcher) GetLatestHeader() (*common.BlockHeader, error) {
	var header *common.BlockHeader
	err := f.client.CallContext(context.TODO(), &header, "eth_getBlockByNumber", "latest", false)
	if err == nil && header == nil {
		return nil, ethereum.NotFound
	}
	return header, err
}

func (f *BlockFetcher) GetHeadersByNumber(numbers []*big.Int) ([]*common.BlockHeader, error) {
	if len(numbers) == 0 {
		return []*common.BlockHeader{}, nil
	}

	methods := make([]rpc.BatchElem, len(numbers))
	results := make([]*common.BlockHeader, len(numbers))

	for i, n := range numbers {
		methods[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{fmt.Sprint("0x", n.Text(16)), false},
			Result: &results[i],
		}
	}

	if err := f.batchCall(methods); err != nil {
		return nil, err
	}

	return results, nil
}

func (f *BlockFetcher) GetTransactionsByHash(hashes []string) ([]*common.Transaction, error) {
	if len(hashes) == 0 {
		return []*common.Transaction{}, nil
	}

	methods := make([]rpc.BatchElem, len(hashes))
	results := make([]*common.Transaction, len(hashes))
	anyResults := make([]any, len(hashes))

	for i, h := range hashes {
		methods[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{h},
			Result: &results[i],
		}
		anyResults = append(anyResults, any(results[i]))
	}

	if err := f.batchCall(methods); err != nil {
		return nil, err
	}

	return results, nil
}

func (f *BlockFetcher) GetTransactionReceipts(transactions []*common.Transaction) ([]*common.TransactionReceipt, error) {
	if len(transactions) == 0 {
		return []*common.TransactionReceipt{}, nil
	}

	methods := make([]rpc.BatchElem, len(transactions))
	results := make([]*common.TransactionReceipt, len(transactions))

	for i, tx := range transactions {
		methods[i] = rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []interface{}{tx.Hash},
			Result: &results[i],
		}
	}

	if err := f.batchCall(methods); err != nil {
		return nil, err
	}

	return results, nil
}

func (f *BlockFetcher) batchCall(methods []rpc.BatchElem) error {
	if err := f.client.BatchCall(methods); err != nil {
		return err
	}

	for _, elem := range methods {
		if elem.Error != nil {
			return elem.Error
		}
		if elem.Result == nil {
			return fmt.Errorf("Received null response for %v", elem.Args[0])
		}
	}

	return nil
}

func (f *BlockFetcher) Fetch() error {
	fetcherErrors := make(chan error)

	go func() {
		for {
			if err := f.FetchAll(); err != nil {
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
