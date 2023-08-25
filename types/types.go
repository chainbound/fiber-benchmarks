package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type ConfirmedObservationRow struct {
	TxHash         string `ch:"tx_hash"`
	FiberTimestamp int64  `ch:"fiber_timestamp"`
	OtherTimestamp int64  `ch:"other_timestamp"`
	Difference     int64  `ch:"difference"`
	BenchmarkID    string `ch:"benchmark_id"`
}

type ObservationStatsRow struct {
	StartTime   time.Time `ch:"start_time"`
	EndTime     time.Time `ch:"end_time"`
	FiberWon    float64   `ch:"fiber_won"`
	Min         float64   `ch:"min"`
	Max         float64   `ch:"max"`
	Mean        float64   `ch:"mean"`
	P1          float64   `ch:"p1"`
	P5          float64   `ch:"p5"`
	P10         float64   `ch:"p10"`
	P15         float64   `ch:"p15"`
	P20         float64   `ch:"p20"`
	P25         float64   `ch:"p25"`
	P30         float64   `ch:"p30"`
	P35         float64   `ch:"p35"`
	P40         float64   `ch:"p40"`
	P45         float64   `ch:"p45"`
	P50         float64   `ch:"p50"`
	P55         float64   `ch:"p55"`
	P60         float64   `ch:"p60"`
	P65         float64   `ch:"p65"`
	P70         float64   `ch:"p70"`
	P75         float64   `ch:"p75"`
	P80         float64   `ch:"p80"`
	P85         float64   `ch:"p85"`
	P90         float64   `ch:"p90"`
	P95         float64   `ch:"p95"`
	P99         float64   `ch:"p99"`
	BenchmarkID string    `ch:"benchmark_id"`
}

type Observation struct {
	// Hash
	Hash common.Hash
	// Timestamp in microseconds
	Timestamp int64
}

// Use a buffer here because we don't want to block transaction sources as this
// would result in bad timestamps.
const OBSERVATION_BUFFER_SIZE = 8192

const (
	numBins  = 22
	binSize  = 1
	minValue = -10
	maxValue = 10
)

func MakeHistogram(data []float64) string {
	if len(data) == 0 {
		return ""
	}
	histogram := make([]int, numBins)

	for _, value := range data {
		binIndex := getBinIndex(value, minValue, maxValue)
		if binIndex >= 0 && binIndex < numBins {
			histogram[binIndex]++
		}
	}

	result := ""
	for i, count := range histogram {
		percentage := float64(count) / float64(len(data)) * 100
		binStart := getBinStart(i)
		binEnd := getBinEnd(i)
		bar := getHistogramBar(int(percentage))

		result += fmt.Sprintf("%3s <-> %3s  %6.2f%%  %4s %s\n", binStart, binEnd, percentage, strconv.Itoa(count), bar)
	}

	return result
}

func getHistogramBar(count int) string {
	barLength := count / 2
	bar := strings.Repeat("█", barLength)
	remainder := count % 2
	if remainder > 0 {
		bar += "▏"
	}
	return bar
}

func getBinIndex(value, minValue, maxValue float64) int {
	if value < minValue {
		return 0
	}
	if value > maxValue {
		return numBins - 1
	}

	return int(math.Ceil(value+maxValue) / binSize)
}

func getBinStart(binIndex int) string {
	if binIndex == 0 {
		return "-∞"
	}
	return fmt.Sprintf("%d", -10+(binIndex-1)*binSize)
}

func getBinEnd(binIndex int) string {
	if binIndex == 0 {
		return fmt.Sprintf("%d", -10)
	}
	if binIndex == numBins-1 {
		return "+∞"
	}

	return fmt.Sprintf("%d", -10+binIndex*binSize)
}
