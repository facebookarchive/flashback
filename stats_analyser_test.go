package flashback

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/facebookgo/ensure"
)

func floatEquals(a float64, b float64, t *testing.T) {
	if !(math.Abs(a-b)/b < 5e-2) {
		fmt.Println(a, b)
	}
	ensure.True(t, math.Abs(a-b)/b < 5e-2)
}

func TestBasics(t *testing.T) {
	statsChan := make(chan OpStat)
	analyser := NewStatsAnalyzer(statsChan)

	for i := 0; i < 10; i += 1 {
		for _, opType := range AllOpTypes {
			statsChan <- OpStat{opType, time.Duration(i) * time.Millisecond, false}
		}
	}
	time.Sleep(100 * time.Millisecond)
	status := analyser.GetStatus()
	ensure.DeepEqual(t, status.OpsExecuted, int64(10*len(AllOpTypes)))
	ensure.DeepEqual(t, status.IntervalOpsExecuted, int64(10*len(AllOpTypes)))
	ensure.DeepEqual(t, status.OpsErrors, int64(0))
	ensure.DeepEqual(t, status.IntervalOpsErrors, int64(0))
	floatEquals(status.OpsPerSec, 689.0, t)
	floatEquals(status.IntervalOpsPerSec, 689.0, t)

	for _, opType := range AllOpTypes {
		ensure.DeepEqual(t, status.Latencies[opType][P50], float64(4))
		ensure.DeepEqual(t, status.Latencies[opType][P70], float64(6))
		ensure.DeepEqual(t, status.Latencies[opType][P95], float64(8))
		ensure.DeepEqual(t, status.Latencies[opType][P99], float64(8))
		ensure.DeepEqual(t, status.MaxLatency[opType], float64(9))
		ensure.DeepEqual(t, status.IntervalLatencies[opType][P50], float64(4))
		ensure.DeepEqual(t, status.IntervalLatencies[opType][P70], float64(6))
		ensure.DeepEqual(t, status.IntervalLatencies[opType][P95], float64(8))
		ensure.DeepEqual(t, status.IntervalLatencies[opType][P99], float64(8))
		ensure.DeepEqual(t, status.IntervalMaxLatency[opType], float64(9))

		ensure.DeepEqual(t, status.Counts[opType], int64(10))
		ensure.DeepEqual(t, status.IntervalCounts[opType], int64(10))

		floatEquals(status.TypeOpsSec[opType], 100.0, t)
		floatEquals(status.IntervalTypeOpsSec[opType], 100.0, t)
	}

	// second interval
	for i := 0; i < 10; i += 1 {
		for _, opType := range AllOpTypes {
			statsChan <- OpStat{opType, time.Duration(i) * time.Millisecond, false}
		}
	}
	statsChan <- OpStat{Insert, 0, true}
	time.Sleep(200 * time.Millisecond)

	status = analyser.GetStatus()
	ensure.DeepEqual(t, status.OpsExecuted, int64(20*len(AllOpTypes))+1)
	ensure.DeepEqual(t, status.IntervalOpsExecuted, int64(10*len(AllOpTypes))+1)
	ensure.DeepEqual(t, status.OpsErrors, int64(1))
	ensure.DeepEqual(t, status.IntervalOpsErrors, int64(1))
	floatEquals(status.OpsPerSec, 458.0, t)
	floatEquals(status.IntervalOpsPerSec, 352.0, t)

	for _, opType := range AllOpTypes {
		if opType == Insert {
			ensure.DeepEqual(t, status.Counts[opType], int64(21))
			ensure.DeepEqual(t, status.IntervalCounts[opType], int64(11))
		} else {
			ensure.DeepEqual(t, status.Counts[opType], int64(20))
			ensure.DeepEqual(t, status.IntervalCounts[opType], int64(10))
			floatEquals(status.TypeOpsSec[opType], 66.6, t)
			floatEquals(status.IntervalTypeOpsSec[opType], 50.0, t)
		}
	}
}

func TestLatencies(t *testing.T) {
	statsChan := make(chan OpStat)
	analyser := NewStatsAnalyzer(statsChan)

	start := 1000
	for _, opType := range AllOpTypes {
		for i := 100; i >= 0; i-- {
			statsChan <- OpStat{opType, time.Duration(start+i) * time.Millisecond, false}
		}
		start += 2000
	}

	time.Sleep(10)
	status := analyser.GetStatus()

	// Check results
	start = 1000
	for _, opType := range AllOpTypes {
		latencies := status.Latencies[opType]
		intervalLatencies := status.IntervalLatencies[opType]
		for i, perc := range latencyPercentiles {
			floatEquals(latencies[i], float64(perc*100.0+float64(start)), t)
			floatEquals(intervalLatencies[i], float64(perc*100.0+float64(start)), t)
		}
		start += 2000
	}

	// -- second round
	start = 2000
	for _, opType := range AllOpTypes {
		for i := 100; i >= 0; i-- {
			statsChan <- OpStat{opType, time.Duration(start+i) * time.Millisecond, false}
		}
		start += 2000
	}
	time.Sleep(10)
	status = analyser.GetStatus()

	start = 2000
	for _, opType := range AllOpTypes {
		latencies := status.Latencies[opType]
		intervalLatencies := status.IntervalLatencies[opType]
		for i, perc := range latencyPercentiles {
			floatEquals(intervalLatencies[i], float64(perc*100.0+float64(start)), t)
		}
		floatEquals(latencies[len(latencies)-1], float64(start+100), t)
		floatEquals(latencies[0], float64(start-1000+100), t)
		start += 2000
	}
}
