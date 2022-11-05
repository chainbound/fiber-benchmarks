package main

import (
	"context"
	"encoding/csv"
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
	fmt.Println("Bloxroute endpoint:", blxrEndpoint)
	fmt.Println("Fiber endpoint:", fiberEndpoint)
	ctx1, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	ctx2, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	go func() {
		if err := runBloxroute(ctx1, blxrEndpoint, blxrKey); err != nil {
			log.Fatal(err)
		}
	}()

	if err := runFiber(ctx2, fiberEndpoint, fiberKey); err != nil {
		log.Fatal(err)
	}

	entries := int64(0)
	sum := int64(0)

	fiberWon := 0
	blxrWon := 0

	// Wait for both goroutines to exit
	time.Sleep(time.Second)
	f, err := os.Create("benchmarks.csv")
	if err != nil {
		log.Fatalln("failed to open file", err)
	}

	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"txHash", "diff"})

	for fh, fts := range fiberMap {
		for bh, bts := range blxrMap {
			if fh == bh {
				diff := bts - fts
				if diff > 0 {
					fiberWon++
				} else {
					blxrWon++
				}

				w.Write([]string{fh.Hex(), fmt.Sprint(diff)})
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

func runBloxroute(ctx context.Context, endpoint, key string) error {
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
		case <-ctx.Done():
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

func runFiber(ctx context.Context, endpoint, key string) error {
	c := fiber.NewClient(endpoint, key)
	defer c.Close()

	if err := c.Connect(ctx); err != nil {
		return err
	}

	sub := make(chan *fiber.Transaction)

	go c.SubscribeNewTxs(nil, sub)

	for tx := range sub {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fiberMap[tx.Hash] = time.Now().UnixMilli()
	}

	return nil
}
