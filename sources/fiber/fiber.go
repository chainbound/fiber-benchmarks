package fiber

import (
	"context"
	"time"

	"github.com/chainbound/fiber-benchmarks/types"
	fiber "github.com/chainbound/fiber-go"
)

type FiberSource struct {
	client *fiber.Client
	done   chan struct{}
}

func NewFiberSource(endpoint, apiKey string) *FiberSource {
	return &FiberSource{
		client: fiber.NewClient(endpoint, apiKey),
		done:   make(chan struct{}),
	}
}

func (f *FiberSource) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := f.client.Connect(ctx); err != nil {
		return err
	}

	return nil
}

// Subscribe to new transactions. This function returns a channel of transactions that will
// close once `Close` gets called.
func (f *FiberSource) SubscribeTransactions() chan *fiber.Transaction {
	ch := make(chan *fiber.Transaction)

	go func() {
		go func() {
			if err := f.client.SubscribeNewTxs(nil, ch); err != nil {
				panic(err)
			}
		}()

		<-f.done
		close(ch)
	}()

	return ch
}

// Subscribe to new transactio hashes. This function returns a BUFFERED channel of transaction hashes that will
// close once `Close` gets called.
func (f *FiberSource) SubscribeTransactionObservations() chan types.Observation {
	hashCh := make(chan types.Observation, types.OBSERVATION_BUFFER_SIZE)

	go func() {
		ch := f.SubscribeTransactions()

		for tx := range ch {
			hashCh <- types.Observation{
				Hash:         tx.Hash,
				Timestamp:    time.Now().UnixMicro(),
				CallDataSize: int64(len(tx.Input)),
				From:         tx.From.Hex(),
				To:           tx.To.Hex(),
			}
		}

		close(hashCh)
	}()

	return hashCh
}

// Subscribe to new execution payloads. This function returns a channel of transactions that will
// close once `Close` gets called.
func (f *FiberSource) SubscribeExecutionPayloads() chan *fiber.ExecutionPayload {
	ch := make(chan *fiber.ExecutionPayload)

	go func() {
		go func() {
			if err := f.client.SubscribeNewExecutionPayloads(ch); err != nil {
				panic(err)
			}
		}()

		<-f.done
		close(ch)
	}()

	return ch
}

func (f *FiberSource) SubscribeBlockObservations() chan types.BlockObservation {
	obsCh := make(chan types.BlockObservation, 16)

	go func() {
		ch := f.SubscribeExecutionPayloads()

		for block := range ch {
			obsCh <- types.BlockObservation{
				Hash:            block.Header.Hash,
				Timestamp:       time.Now().UnixMicro(),
				TransactionsLen: len(block.Transactions),
			}
		}

		close(obsCh)

	}()

	return obsCh
}

func (f *FiberSource) Close() error {
	close(f.done)
	return f.client.Close()
}
