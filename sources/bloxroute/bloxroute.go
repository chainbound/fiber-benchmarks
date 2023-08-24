package bloxroute

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"

	"github.com/chainbound/fiber-benchmarks/types"
)

type BloxrouteSource struct {
	endpoint string
	key      string
	dialer   *websocket.Dialer

	done chan struct{}
}

type blxrResponse[T any] struct {
	Params struct {
		Result T
	}
}

// A bloxroute transaction
type Transaction struct {
	TxHash     common.Hash
	TxContents any
}

// A bloxroute block
type Block struct {
	Hash         common.Hash
	Header       any
	Transactions []any
}

func NewBloxrouteSource(endpoint, apiKey string) *BloxrouteSource {
	return &BloxrouteSource{
		endpoint: endpoint,
		key:      apiKey,
		dialer:   websocket.DefaultDialer,
	}
}

// Subscribe to new transactions.
func (b *BloxrouteSource) SubscribeTransactions() (chan *Transaction, error) {
	ch := make(chan *Transaction)
	subReq := `{"id": 1, "method": "subscribe", "params": ["newTxs", {"include": ["tx_hash", "tx_contents"]}]}`

	sub, _, err := b.dialer.Dial(b.endpoint, http.Header{"Authorization": []string{b.key}})
	if err != nil {
		return nil, err
	}

	err = sub.WriteMessage(websocket.TextMessage, []byte(subReq))
	if err != nil {
		return nil, err
	}

	// Read the first (confirmation) message
	_, _, err = sub.ReadMessage()
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-b.done:
				// We're done, close the websocket connection and return
				close(ch)
				sub.Close()
				return
			default:
			}

			var decoded blxrResponse[Transaction]
			_, msg, err := sub.ReadMessage()
			if err != nil {
				log.Println(err)
				continue
			}

			if err := json.Unmarshal(msg, &decoded); err != nil {
				log.Println(err)
				continue
			}

			ch <- &decoded.Params.Result
		}
	}()

	return ch, nil
}

// Subscribe to new transaction hashes.
func (b *BloxrouteSource) SubscribeTransactionObservations() chan types.Observation {
	hashCh := make(chan types.Observation, types.OBSERVATION_BUFFER_SIZE)

	ch, err := b.SubscribeTransactions()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for tx := range ch {
			hashCh <- types.Observation{
				Hash:      tx.TxHash,
				Timestamp: time.Now().UnixMicro(),
			}
		}
	}()

	return hashCh
}

func (b *BloxrouteSource) SubscribeExecutionPayloads() (chan *Block, error) {
	ch := make(chan *Block)
	subReq := `{"id": 1, "method": "subscribe", "params": ["bdnBlocks", {"include": ["hash", "header", "transactions"]}]}`

	sub, _, err := b.dialer.Dial(b.endpoint, http.Header{"Authorization": []string{b.key}})
	if err != nil {
		return nil, err
	}

	err = sub.WriteMessage(websocket.TextMessage, []byte(subReq))
	if err != nil {
		return nil, err
	}

	// Read the first (confirmation) message
	_, _, err = sub.ReadMessage()
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-b.done:
				// We're done, close the websocket connection and return
				close(ch)
				sub.Close()
				return
			default:
			}

			var decoded blxrResponse[Block]
			_, msg, err := sub.ReadMessage()
			if err != nil {
				log.Println(err)
				continue
			}

			if err := json.Unmarshal(msg, &decoded); err != nil {
				log.Println(err)
				continue
			}

			ch <- &decoded.Params.Result
		}
	}()

	return ch, nil
}

// Closes the WebSocket connection and all open subscriptions
func (b *BloxrouteSource) Close() {
	close(b.done)
}
