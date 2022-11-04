package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	fiber "github.com/chainbound/fiber-go"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var blxrMap = make(map[common.Hash]int64, 10000)
var fiberMap = make(map[common.Hash]int64, 10000)

func main() {
	godotenv.Load()

	blxrKey := os.Getenv("BLXR_API_KEY")
	fiberKey := os.Getenv("FIBER_API_KEY")

	if blxrKey == "" || fiberKey == "" {
		log.Fatal("set API keys")
	}

	blxrEndpoint := os.Getenv("BLXR_ENDPOINT")
	fiberEndpoint := os.Getenv("FIBER_ENDPOINT")

	if blxrEndpoint == "" || fiberEndpoint == "" {
		log.Fatal("set endpoints")
	}

	duration, err := time.ParseDuration(os.Getenv("DURATION"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Running benchmark for", duration, "...")

	done := make(chan bool)

	go func() {
		<-time.After(duration)
		close(done)
	}()

	go func() {
		if err := runBloxroute(blxrEndpoint, blxrKey, done); err != nil {
			log.Fatal(err)
		}
	}()

	if err := runFiber(fiberEndpoint, fiberKey, done); err != nil {
		log.Fatal(err)
	}

	entries := int64(0)
	sum := int64(0)

	fiberWon := 0
	blxrWon := 0

	for fh, fts := range fiberMap {
		for bh, bts := range blxrMap {
			if fh == bh {
				diff := bts - fts
				if diff > 0 {
					fiberWon++
				} else {
					blxrWon++
				}
				sum += diff
				entries++
			}
		}
	}

	fmt.Printf("Fiber was %dms faster on average\n", sum/entries)
	fmt.Println("Fiber won", fiberWon)
	fmt.Println("Bloxroute won", blxrWon)

	fmt.Printf("Fiber won %.2f%% of the time\n", float64(fiberWon)/float64(entries)*100)
}

type BlxrMsg struct {
	Params struct {
		Result struct {
			TxHash common.Hash
		}
	}
}

func runBloxroute(endpoint, key string, done chan bool) error {
	dialer := websocket.DefaultDialer
	sub, _, err := dialer.Dial(endpoint, http.Header{"Authorization": []string{key}})
	if err != nil {
		return err
	}

	subReq := `{"id": 1, "method": "subscribe", "params": ["newTxs", {"include": ["tx_hash", "tx_contents"]}]}`

	err = sub.WriteMessage(websocket.TextMessage, []byte(subReq))
	if err != nil {
		return err
	}

	for {
		select {
		case <-done:
			return nil
		default:
		}
		var decoded BlxrMsg
		_, msg, err := sub.ReadMessage()
		if err != nil {
			log.Println(err)
		}

		ts := time.Now().UnixMilli()

		json.Unmarshal(msg, &decoded)
		blxrMap[decoded.Params.Result.TxHash] = ts
	}
}

func runFiber(endpoint, key string, done chan bool) error {
	c := fiber.NewClient(endpoint, key)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		return err
	}

	sub := make(chan *fiber.Transaction)

	go c.SubscribeNewTxs(nil, sub)

	for tx := range sub {
		select {
		case <-done:
			return nil
		default:
		}

		fiberMap[tx.Hash] = time.Now().UnixMilli()
	}

	return nil
}
