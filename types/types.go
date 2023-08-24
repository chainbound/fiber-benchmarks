package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

type Observation struct {
	// Hash
	Hash common.Hash
	// Timestamp in microseconds
	Timestamp int64
}

// Use a buffer here because we don't want to block transaction sources as this
// would result in bad timestamps.
const OBSERVATION_BUFFER_SIZE = 4096

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

	fmt.Println(data)

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
