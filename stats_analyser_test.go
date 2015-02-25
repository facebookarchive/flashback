package flashback

import (
	"math"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestStatsAnalyzer(t *testing.T) {
	TestingT(t)
}

type TestStatsAnalyzerSuite struct{}

var _ = Suite(&TestStatsAnalyzerSuite{})

func floatEquals(a float64, b float64, c *C) {
	c.Assert(math.Abs(a-b)/b < 5e-2, Equals, true)
}

func (s *TestStatsAnalyzerSuite) TestBasics(c *C) {
	statsChan := make(chan OpStat)
	analyser := NewStatsAnalyzer(statsChan)

	for i := 0; i < 10; i += 1 {
		for _, opType := range AllOpTypes {
			statsChan <- OpStat{opType, time.Duration(i) * time.Millisecond, false}
		}
	}
	time.Sleep(100 * time.Millisecond)
	status := analyser.GetStatus()
	c.Assert(status.OpsExecuted, Equals, int64(10*len(AllOpTypes)))
	c.Assert(status.IntervalOpsExecuted, Equals, int64(10*len(AllOpTypes)))
	c.Assert(status.OpsErrors, Equals, int64(0))
	c.Assert(status.IntervalOpsErrors, Equals, int64(0))
	floatEquals(status.OpsPerSec, 600.0, c)
	floatEquals(status.IntervalOpsPerSec, 600.0, c)

	for _, opType := range AllOpTypes {
		c.Assert(status.Latencies[opType][P50], Equals, float64(4))
		c.Assert(status.Latencies[opType][P70], Equals, float64(6))
		c.Assert(status.Latencies[opType][P95], Equals, float64(8))
		c.Assert(status.Latencies[opType][P99], Equals, float64(8))
		c.Assert(status.MaxLatency[opType], Equals, float64(9))
		c.Assert(status.IntervalLatencies[opType][P50], Equals, float64(4))
		c.Assert(status.IntervalLatencies[opType][P70], Equals, float64(6))
		c.Assert(status.IntervalLatencies[opType][P95], Equals, float64(8))
		c.Assert(status.IntervalLatencies[opType][P99], Equals, float64(8))
		c.Assert(status.IntervalMaxLatency[opType], Equals, float64(9))

		c.Assert(status.Counts[opType], Equals, int64(10))
		c.Assert(status.IntervalCounts[opType], Equals, int64(10))

		floatEquals(status.TypeOpsSec[opType], 100.0, c)
		floatEquals(status.IntervalTypeOpsSec[opType], 100.0, c)
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
	c.Assert(status.OpsExecuted, Equals, int64(20*len(AllOpTypes))+1)
	c.Assert(status.IntervalOpsExecuted, Equals, int64(10*len(AllOpTypes))+1)
	c.Assert(status.OpsErrors, Equals, int64(1))
	c.Assert(status.IntervalOpsErrors, Equals, int64(1))
	floatEquals(status.OpsPerSec, 400.0, c)
	floatEquals(status.IntervalOpsPerSec, 300.0, c)

	for _, opType := range AllOpTypes {
		if opType == Insert {
			c.Assert(status.Counts[opType], Equals, int64(21))
			c.Assert(status.IntervalCounts[opType], Equals, int64(11))
		} else {
			c.Assert(status.Counts[opType], Equals, int64(20))
			c.Assert(status.IntervalCounts[opType], Equals, int64(10))
			floatEquals(status.TypeOpsSec[opType], 66.6, c)
			floatEquals(status.IntervalTypeOpsSec[opType], 50.0, c)
		}
	}
}

func (s *TestStatsAnalyzerSuite) TestLatencies(c *C) {
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
			floatEquals(latencies[i], float64(perc*100.0+float64(start)), c)
			floatEquals(intervalLatencies[i], float64(perc*100.0+float64(start)), c)
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
			floatEquals(intervalLatencies[i], float64(perc*100.0+float64(start)), c)
		}
		floatEquals(latencies[len(latencies)-1], float64(start+100), c)
		floatEquals(latencies[0], float64(start-1000+100), c)
		start += 2000
	}
}
