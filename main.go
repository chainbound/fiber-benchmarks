package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chainbound/fiber-benchmarks/sources/bloxroute"
	"github.com/chainbound/fiber-benchmarks/sources/fiber"
	"github.com/chainbound/fiber-benchmarks/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/montanaflynn/stats"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

type config struct {
	fiberEndpoint string
	fiberKey      string
	blxrEndpoint  string
	blxrKey       string

	crossCheck    bool
	interval      time.Duration
	intervalCount int
	logMissing    bool
	logFile       string
	tailWindow    time.Duration
}

type TransactionSource interface {
	SubscribeTransactionObservations() chan types.Observation
}

func main() {
	var config config

	log := NewLogger("benchmark")

	app := &cli.App{
		Name:  "fiber-benchmark",
		Usage: "Benchmark Fiber against other data sources",
		Commands: []*cli.Command{
			{
				Name:  "transactions",
				Usage: "Benchmark transaction streams",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "cross-check",
						Usage:       "Cross-check transaction observations with confirmed execution payloads",
						Value:       true,
						Destination: &config.crossCheck,
					},
					&cli.BoolFlag{
						Name:        "log-missing",
						Usage:       "Log transactions that are missing from the other stream",
						Value:       true,
						Destination: &config.logMissing,
					},
					&cli.DurationFlag{
						Name:        "tail-window",
						Usage:       "Duration of the tail window to wait for transactions after an interval ends",
						Value:       200 * time.Millisecond,
						Destination: &config.tailWindow,
					},
				},
				Action: func(c *cli.Context) error {
					if err := runTransactionBenchmark(&config); err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:  "blocks",
				Usage: "Benchmark block streams",
				Action: func(c *cli.Context) error {
					fmt.Println("Coming soon")
					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "fiber-endpoint",
				Usage:       "Fiber API endpoint",
				Destination: &config.fiberEndpoint,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "fiber-key",
				Usage:       "Fiber API key",
				Destination: &config.fiberKey,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "blxr-endpoint",
				Usage:       "Bloxroute API endpoint",
				Destination: &config.blxrEndpoint,
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "blxr-key",
				Usage:       "Bloxroute API key",
				Destination: &config.blxrKey,
				Required:    false,
			},
			&cli.DurationFlag{
				Name:        "interval",
				Usage:       "Duration of each interval",
				Required:    true,
				Destination: &config.interval,
			},
			&cli.IntFlag{
				Name:        "interval-count",
				Usage:       "Number of intervals to run",
				Value:       1,
				Destination: &config.intervalCount,
			},
			&cli.StringFlag{
				Name:        "log-file",
				Usage:       "File to save detailed logs",
				Destination: &config.logFile,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("Failed to run app, exiting")
	}
}

type IntervalResult struct {
	Mean                    float64
	Median                  float64
	P10, P25, P75, P90, P99 float64
	Min, Max                float64

	// Number of transactions seen by Fiber
	FiberCount int
	// Number of transactions seen by the other source
	OtherCount int
}

type TransactionBenchmarker struct {
	config *config
	logger zerolog.Logger

	fiberSource     *fiber.FiberSource
	otherSource     TransactionSource
	otherSourceName string

	csvWriter *csv.Writer

	// Results of all intervals
	// results []IntervalResult
}

func runTransactionBenchmark(config *config) error {
	// - For each interval, we collect all data from both streams
	// - At the end of the interval, we print the stats and save the result
	// - At the end of the benchmark, we print the overall stats
	logger := NewLogger("tx-bencher")
	var writer *csv.Writer
	if config.logFile != "" {
		f, err := os.Create(config.logFile)
		if err != nil {
			log.Fatalln("failed to open file", err)
		}

		defer f.Close()

		writer = csv.NewWriter(f)
		defer writer.Flush()

		writer.Write([]string{"tx_hash", "fiber_timestamp", "other_timestamp", "diff"})
	}

	fiberSource := fiber.NewFiberSource(config.fiberEndpoint, config.fiberKey)
	if err := fiberSource.Connect(); err != nil {
		return err
	}

	var otherSource TransactionSource
	var otherSourceName string

	if config.blxrEndpoint != "" && config.blxrKey != "" {
		otherSource = bloxroute.NewBloxrouteSource(config.blxrEndpoint, config.blxrKey)
		otherSourceName = "bloxroute"
	}

	benchmarker := &TransactionBenchmarker{
		config:          config,
		logger:          logger,
		fiberSource:     fiberSource,
		otherSource:     otherSource,
		otherSourceName: otherSourceName,
		csvWriter:       writer,
	}

	benchmarker.Run()

	return nil
}

func (b *TransactionBenchmarker) Run() {
	for i := 0; i < b.config.intervalCount; i++ {
		b.logger.Info().Int("interval", i+1).Msg("Running benchmark interval")
		b.runInterval()
	}
}

// Runs the interval
func (b *TransactionBenchmarker) runInterval() {
	// Setup
	var (
		fiberMap = make(map[common.Hash]int64)
		otherMap = make(map[common.Hash]int64)
		truthMap = make(map[common.Hash]struct{})

		// Keep track of whether we're in the tail window
		inTailWindow = false

		fiberStream   = b.fiberSource.SubscribeTransactionObservations()
		otherStream   = b.otherSource.SubscribeTransactionObservations()
		payloadStream = b.fiberSource.SubscribeExecutionPayloads()
	)

	// Initialize interval timer
	timer := time.NewTimer(b.config.interval)

loop:
	for {
		select {
		case <-timer.C:
			// TODO: handle tail window
			inTailWindow = true
			break loop
		case fiberObs := <-fiberStream:
			// If we're not in the tail window, continue as usual
			if !inTailWindow {
				if _, ok := fiberMap[fiberObs.Hash]; !ok {
					fiberMap[fiberObs.Hash] = fiberObs.Timestamp
				} else {
					b.logger.Warn().Str("hash", fiberObs.Hash.Hex()).Str("source", "fiber").Msg("Duplicate hash during interval")
				}
			}
		case otherObs := <-otherStream:
			if !inTailWindow {
				if _, ok := otherMap[otherObs.Hash]; !ok {
					otherMap[otherObs.Hash] = otherObs.Timestamp
				} else {
					b.logger.Warn().Str("hash", otherObs.Hash.Hex()).Str("source", b.otherSourceName).Msg("Duplicate hash during interval")
				}
			}
		case payload := <-payloadStream:
			if b.config.crossCheck {
				b.logger.Info().Int("number", int(payload.Header.Number)).Msg("Recording execution payload transactions")
				for _, tx := range payload.Transactions {
					truthMap[tx.Hash] = struct{}{}
				}
			}
		}
	}

	b.processIntervalResults(fiberMap, otherMap, truthMap)
}

func (b *TransactionBenchmarker) processIntervalResults(fiberMap, otherMap map[common.Hash]int64, truthMap map[common.Hash]struct{}) {
	diffMap := make(map[common.Hash]float64, len(truthMap))
	differences := make([]float64, len(truthMap))

	for hash := range truthMap {
		var (
			fiberSaw = false
			otherSaw = false
		)

		fiberTs, ok := fiberMap[hash]
		if ok {
			fiberSaw = true
		}

		otherTs, ok := otherMap[hash]
		if ok {
			otherSaw = true
		}

		microDiff := otherTs - fiberTs
		milliDiff := float64(microDiff) / 1000

		switch {
		case fiberSaw && otherSaw:
			// Both saw the transaction. Record the difference
			diffMap[hash] = milliDiff
			differences = append(differences, milliDiff)

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(fiberTs), fmt.Sprint(otherTs), fmt.Sprint(microDiff)})
			}
		case fiberSaw && !otherSaw:
			// Only Fiber saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("Fiber saw transaction but %s did not", b.otherSourceName))
			}

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(fiberTs), fmt.Sprint(0), fmt.Sprint(microDiff)})
			}
		case !fiberSaw && otherSaw:
			// Only the other source saw the transaction
			if b.config.logMissing {
				b.logger.Warn().Str("hash", hash.Hex()).Msg(fmt.Sprintf("%s saw transaction but fiber did not", b.otherSourceName))
			}

			if b.csvWriter != nil {
				b.csvWriter.Write([]string{hash.Hex(), fmt.Sprint(0), fmt.Sprint(otherTs), fmt.Sprint(microDiff)})
			}
		}
	}

	fmt.Println(types.MakeHistogram(differences))
	b.logger.Info().Msg(fmt.Sprintf("Fiber total observations: %d", len(fiberMap)))
	b.logger.Info().Msg(fmt.Sprintf("Bloxroute total observations: %d", len(fiberMap)))
	b.printStats(differences)

}

func (b *TransactionBenchmarker) printStats(differences []float64) {
	mean, err := stats.Mean(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate mean")
	}

	median, err := stats.Median(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate median")
	}

	stdev, err := stats.StandardDeviation(differences)
	if err != nil {
		b.logger.Error().Err(err).Msg("Failed to calculate stdev")
	}

	b.logger.Info().Msg(fmt.Sprintf("Mean: %.4fms", mean))
	b.logger.Info().Msg(fmt.Sprintf("Median: %.4fms", median))
	b.logger.Info().Msg(fmt.Sprintf("Stdev: %.4fms", stdev))
}

// type BlxrBlock struct {
// 	Params struct {
// 		Result struct {
// 			Hash         common.Hash
// 			Header       any
// 			Transactions []any
// 		}
// 	}
// }

// func runBloxrouteBlocks(ctx context.Context, endpoint, key string) error {
// 	dialer := websocket.DefaultDialer
// 	sub, _, err := dialer.Dial(endpoint, http.Header{"Authorization": []string{key}})
// 	if err != nil {
// 		return err
// 	}

// 	subReq := `{"id": 1, "method": "subscribe", "params": ["bdnBlocks", {"include": ["hash", "header", "transactions"]}]}`

// 	err = sub.WriteMessage(websocket.TextMessage, []byte(subReq))
// 	if err != nil {
// 		return err
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}
// 		var decoded BlxrBlock
// 		_, msg, err := sub.ReadMessage()
// 		if err != nil {
// 			log.Println(err)
// 		}

// 		ts := time.Now().UnixMilli()

// 		json.Unmarshal(msg, &decoded)
// 		hash := decoded.Params.Result.Hash

// 		fmt.Printf("[%d] [Bloxroute] New block: %s (%d)\n", ts, hash, len(decoded.Params.Result.Transactions))

// 		if _, ok := blxrMap[hash]; !ok {
// 			blxrMap[hash] = ts
// 		}
// 	}
// }

// func runFiber(ctx context.Context, endpoint, key string) error {
// 	c := fiber.NewClient(endpoint, key)

// 	if err := c.Connect(ctx); err != nil {
// 		return err
// 	}
// 	defer c.Close()
// 	fmt.Println("Fiber connected")

// 	sub := make(chan *fiber.Transaction)

// 	go c.SubscribeNewTxs(nil, sub)

// 	for tx := range sub {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}

// 		if _, ok := fiberMap[tx.Hash]; !ok {
// 			fiberMap[tx.Hash] = time.Now().UnixMilli()
// 		}
// 	}

// 	return nil
// }

// func runFiberBlocks(ctx context.Context, endpoint, key string) error {
// 	c := fiber.NewClient(endpoint, key)

// 	if err := c.Connect(ctx); err != nil {
// 		return err
// 	}
// 	defer c.Close()
// 	fmt.Println("Fiber connected")

// 	sub := make(chan *fiber.ExecutionPayload)

// 	go c.SubscribeNewExecutionPayloads(sub)

// 	for tx := range sub {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}

// 		ts := time.Now().UnixMilli()
// 		lastTs = ts

// 		fmt.Printf("[%d] [Fiber] New block: %s (%d)\n", ts, tx.Header.Hash, len(tx.Transactions))

// 		if _, ok := fiberMap[tx.Header.Hash]; !ok {
// 			fiberMap[tx.Header.Hash] = ts
// 		}
// 	}

// 	return nil
// }

// func runFiberBeaconBlocks(ctx context.Context, endpoint, key string) error {
// 	c := fiber.NewClient(endpoint, key)

// 	if err := c.Connect(ctx); err != nil {
// 		return err
// 	}
// 	defer c.Close()
// 	fmt.Println("Fiber connected")

// 	sub := make(chan *fiber.BeaconBlock)

// 	go c.SubscribeNewBeaconBlocks(sub)

// 	for block := range sub {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}

// 		ts := time.Now().UnixMilli()
// 		lastTs = ts

// 		slot := big.NewInt(int64(block.Slot))

// 		fmt.Printf("[%d] [Fiber] New beacon block: %s\n", ts, block.StateRoot)

// 		hash := common.BigToHash(slot)
// 		if _, ok := fiberMap[hash]; !ok {
// 			fiberMap[hash] = ts
// 		}
// 	}

// 	return nil
// }

// type BeaconHead struct {
// 	Slot  string
// 	Block string
// }

// func runBeaconBlocks(ctx context.Context, endpoint string) error {
// 	req, err := http.NewRequest("GET", endpoint+"/eth/v1/events?topics=block", nil)
// 	if err != nil {
// 		return err
// 	}

// 	req.Header.Set("Cache-Control", "no-cache")
// 	req.Header.Set("Accept", "text/event-stream")
// 	req.Header.Set("Connection", "keep-alive")

// 	client := &http.Client{}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return err
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}

// 		data := make([]byte, 1024)
// 		var bh BeaconHead
// 		_, err := resp.Body.Read(data)
// 		if err != nil {
// 			return err
// 		}

// 		ts := time.Now().UnixMilli()

// 		fmt.Println("Diff:", ts-lastTs)

// 		data = bytes.Trim(data[17:], "\x00")
// 		if err := json.Unmarshal(data, &bh); err != nil {
// 			fmt.Println("Error unmarshalling beacon head:", err)
// 			continue
// 		}

// 		slot, _ := new(big.Int).SetString(bh.Slot, 10)

// 		hash := common.BigToHash(slot)

// 		fmt.Printf("[%d] [Beacon] New block: %s\n", ts, bh.Block)
// 		if _, ok := beaconMap[hash]; !ok {
// 			beaconMap[hash] = ts
// 		}
// 	}
// }

// func runFiber2(ctx context.Context, endpoint, key string) error {
// 	c := fiber.NewClient(endpoint, key)

// 	if err := c.Connect(ctx); err != nil {
// 		return err
// 	}
// 	defer c.Close()
// 	fmt.Println("Fiber connected")

// 	sub := make(chan *fiber.Transaction)

// 	go c.SubscribeNewTxs(nil, sub)

// 	for tx := range sub {
// 		select {
// 		case <-ctx.Done():
// 			return nil
// 		default:
// 		}

// 		if _, ok := fiberMap2[tx.Hash]; !ok {
// 			fiberMap2[tx.Hash] = time.Now().UnixMilli()
// 		}
// 	}

// 	return nil
// }
