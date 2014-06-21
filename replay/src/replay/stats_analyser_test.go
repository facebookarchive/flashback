package replay

import (
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func TestStatsAnalyser(t *testing.T) {
	TestingT(t)
}

type TestStatsAnalyserSuite struct{}

var _ = Suite(&TestStatsAnalyserSuite{})

func (s *TestStatsAnalyserSuite) TestBasics(c *C) {
	opsExecuted := int64(0)
	latencyChan := make(chan Latency)

	analyser := NewStatsAnalyser(
		[]*StatsCollector{}, &opsExecuted, latencyChan, 1000,
	)

	for _, latencyList := range analyser.latencies {
		c.Assert(latencyList, HasLen, 0)
	}
	for i := 0; i < 10; i += 1 {
		for _, opType := range AllOpTypes {
			latencyChan <- Latency{opType, time.Duration(i)}
		}
	}
	// Need to sleep for a while to make sure the channel got everything
	time.Sleep(10)
	for _, latencyList := range analyser.latencies {
		c.Assert(latencyList, HasLen, 10)
	}
}

func (s *TestStatsAnalyserSuite) TestLatencies(c *C) {
	opsExecuted := int64(0)
	latencyChan := make(chan Latency)

	analyser := NewStatsAnalyser(
		[]*StatsCollector{}, &opsExecuted, latencyChan, 1000,
	)

	start := 1000
	for _, opType := range AllOpTypes {
		for i := 100; i >= 0; i-- {
			latencyChan <- Latency{opType, time.Duration(start + i)}
		}
		start += 2000
	}

	time.Sleep(10)
	status := analyser.GetStatus()

	// Check results
	start = 1000
	for _, opType := range AllOpTypes {
		sinceLast := status.SinceLastLatencies[opType]
		allTime := status.AllTimeLatencies[opType]
		for i, perc := range LatencyPercentiles {
			c.Assert(sinceLast[i], Equals, int64(perc+start))
			c.Assert(allTime[i], Equals, int64(perc+start))
		}
		start += 2000
	}

	// -- second round
	start = 2000
	for _, opType := range AllOpTypes {
		for i := 100; i >= 0; i-- {
			latencyChan <- Latency{opType, time.Duration(start + i)}
		}
		start += 2000
	}
	time.Sleep(10)
	status = analyser.GetStatus()

	start = 2000
	for _, opType := range AllOpTypes {
		sinceLast := status.SinceLastLatencies[opType]
		allTime := status.AllTimeLatencies[opType]
		for i, perc := range LatencyPercentiles {
			c.Assert(sinceLast[i], Equals, int64(perc+start))
		}
		c.Assert(allTime[len(allTime)-1], Equals, int64(start+100))
		c.Assert(allTime[0], Equals, int64(start-1000+100))
		start += 2000
	}
}
