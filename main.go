package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	fiber "github.com/chainbound/fiber-go"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/montanaflynn/stats"
)

var blxrMap = make(map[common.Hash]int64, 10000)
var fiberMap = make(map[common.Hash]int64, 10000)
var fiberMap2 = make(map[common.Hash]int64, 10000)

var modeFlag = flag.String("mode", "normal", "Mode to run in. Either 'normal' or 'fiber'")
var streamFlag = flag.String("stream", "transactions", "Stream to listen to. Either 'transactions' or 'blocks'")

func main() {
	flag.Parse()
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

	ctx1, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	ctx2, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	entries := int64(0)
	sum := int64(0)

	fiberWon := 0
	fiber2Won := 0
	blxrWon := 0

	f, err := os.Create("benchmarks.csv")
	if err != nil {
		log.Fatalln("failed to open file", err)
	}

	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"txHash", "diff"})

	var diffs []float64

	if *modeFlag == "fiber" {
		fmt.Println("Running in Fiber benchmark mode")
		fmt.Println("Running benchmark for", duration, "...")

		fiberEndpoint2 := os.Getenv("FIBER_ENDPOINT_2")
		fmt.Println("Fiber endpoints:", fiberEndpoint, fiberEndpoint2)
		go func() {
			if err := runFiber(ctx2, fiberEndpoint, fiberKey); err != nil {
				log.Fatal("running fiber 1", err)
			}
		}()
		if err := runFiber2(ctx2, fiberEndpoint2, fiberKey); err != nil {
			log.Fatal("running fiber 2", err)
		}
		// Wait for both goroutines to exit
		time.Sleep(time.Second)

		fmt.Println("Fiber saw", len(fiberMap), "txs")
		fmt.Println("Fiber 2 saw", len(fiberMap2), "txs")

		for fh, fts := range fiberMap {
			for bh, bts := range fiberMap2 {
				if fh == bh {
					diff := bts - fts
					if diff > 0 {
						fiberWon++
					} else {
						fiber2Won++
					}

					w.Write([]string{fh.Hex(), fmt.Sprint(diff)})
					diffs = append(diffs, float64(diff))
					sum += diff
					entries++
				}
			}
		}
	} else {
		fmt.Println("Running benchmark for", duration, "...")
		fmt.Println("Bloxroute endpoint:", blxrEndpoint)
		fmt.Println("Fiber endpoint:", fiberEndpoint)

		if *streamFlag == "transactions" {
			fmt.Println("Stream type:", *streamFlag)
			go func() {
				if err := runBloxroute(ctx1, blxrEndpoint, blxrKey); err != nil {
					log.Fatal("running bloxroute", err)
				}
			}()

			if err := runFiber(ctx2, fiberEndpoint, fiberKey); err != nil {
				log.Fatal("running fiber", err)
			}
		} else if *streamFlag == "blocks" {
			fmt.Println("Stream type:", *streamFlag)
			go func() {
				if err := runBloxrouteBlocks(ctx1, blxrEndpoint, blxrKey); err != nil {
					log.Fatal("running bloxroute", err)
				}
			}()
			if err := runFiberBlocks(ctx2, fiberEndpoint, fiberKey); err != nil {
				log.Fatal("running fiber", err)
			}
		}

		// Wait for both goroutines to exit
		time.Sleep(time.Second)

		for fh, fts := range fiberMap {
			for bh, bts := range blxrMap {
				if fh == bh {
					diff := bts - fts
					if diff > 0 {
						fiberWon++
					} else {
						if diff < -30 {
							fmt.Println(fh, diff)
						}
						blxrWon++
					}

					w.Write([]string{fh.Hex(), fmt.Sprint(diff)})
					diffs = append(diffs, float64(diff))
					sum += diff
					entries++
				}
			}
		}
	}

	mean, err := stats.Mean(diffs)
	if err != nil {
		log.Fatal(err)
	}
	max, err := stats.Max(diffs)
	if err != nil {
		log.Fatal(err)
	}
	min, err := stats.Min(diffs)
	if err != nil {
		log.Fatal(err)
	}

	median, err := stats.Median(diffs)
	if err != nil {
		log.Fatal(err)
	}

	p10, err := stats.Percentile(diffs, 10)
	if err != nil {
		log.Fatal(err)
	}

	p25, err := stats.Percentile(diffs, 25)
	if err != nil {
		log.Fatal(err)
	}

	p75, err := stats.Percentile(diffs, 75)
	if err != nil {
		log.Fatal(err)
	}

	p90, err := stats.Percentile(diffs, 90)
	if err != nil {
		log.Fatal(err)
	}

	std, err := stats.StandardDeviation(diffs)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	fmt.Println("========== STATS =============")
	fmt.Println("Fiber messages seen:", len(fiberMap))
	fmt.Println("Bloxroute messages seen:", len(blxrMap))
	fmt.Printf("Mean difference: %.2fms\n", mean)
	fmt.Printf("Median difference: %.2fms\n", median)
	fmt.Printf("P10 difference: %.2fms\n", p10)
	fmt.Printf("P25 difference: %.2fms\n", p25)
	fmt.Printf("P75 difference: %.2fms\n", p75)
	fmt.Printf("P90 difference: %.2fms\n", p90)
	fmt.Printf("Max difference: %.2fms\n", max)
	fmt.Printf("Min difference: %.2fms\n", min)
	fmt.Printf("Stdev: %.2fms\n", std)

	fmt.Println()
	fmt.Println("========== RESULT =============")
	fmt.Printf("Fiber won %.2f%% of the time\n", float64(fiberWon)/float64(entries)*100)
}

type BlxrTransaction struct {
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
		var decoded BlxrTransaction
		_, msg, err := sub.ReadMessage()
		if err != nil {
			log.Println(err)
		}

		ts := time.Now().UnixMilli()

		json.Unmarshal(msg, &decoded)
		hash := decoded.Params.Result.TxHash

		if _, ok := blxrMap[hash]; !ok {
			blxrMap[hash] = ts
		}
	}
}

type BlxrBlock struct {
	Params struct {
		Result struct {
			Hash         common.Hash
			Header       any
			Transactions []any
		}
	}
}

func runBloxrouteBlocks(ctx context.Context, endpoint, key string) error {
	dialer := websocket.DefaultDialer
	sub, _, err := dialer.Dial(endpoint, http.Header{"Authorization": []string{key}})
	if err != nil {
		return err
	}

	subReq := `{"id": 1, "method": "subscribe", "params": ["newBlocks", {"include": ["hash", "header", "transactions"]}]}`

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
		var decoded BlxrBlock
		_, msg, err := sub.ReadMessage()
		if err != nil {
			log.Println(err)
		}

		ts := time.Now().UnixMilli()

		json.Unmarshal(msg, &decoded)
		hash := decoded.Params.Result.Hash

		fmt.Printf("[%d] [Bloxroute] New block: %s (%d)\n", ts, hash, len(decoded.Params.Result.Transactions))

		if _, ok := blxrMap[hash]; !ok {
			blxrMap[hash] = ts
		}
	}
}

func runFiber(ctx context.Context, endpoint, key string) error {
	c := fiber.NewClient(endpoint, key)

	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer c.Close()
	fmt.Println("Fiber connected")

	sub := make(chan *fiber.Transaction)

	go c.SubscribeNewTxs(nil, sub)

	for tx := range sub {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if _, ok := fiberMap[tx.Hash]; !ok {
			fiberMap[tx.Hash] = time.Now().UnixMilli()
		}
	}

	return nil
}

func runFiberBlocks(ctx context.Context, endpoint, key string) error {
	c := fiber.NewClient(endpoint, key)

	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer c.Close()
	fmt.Println("Fiber connected")

	sub := make(chan *fiber.Block)

	go c.SubscribeNewBlocks(sub)

	for tx := range sub {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		ts := time.Now().UnixMilli()

		fmt.Printf("[%d] [Fiber] New block: %s (%d)\n", ts, tx.Hash, len(tx.Transactions))

		if _, ok := fiberMap[tx.Hash]; !ok {
			fiberMap[tx.Hash] = ts
		}
	}

	return nil
}

func runFiber2(ctx context.Context, endpoint, key string) error {
	c := fiber.NewClient(endpoint, key)

	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer c.Close()
	fmt.Println("Fiber connected")

	sub := make(chan *fiber.Transaction)

	go c.SubscribeNewTxs(nil, sub)

	for tx := range sub {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if _, ok := fiberMap2[tx.Hash]; !ok {
			fiberMap2[tx.Hash] = time.Now().UnixMilli()
		}
	}

	return nil
}
