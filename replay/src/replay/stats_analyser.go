package replay

import (
	"sort"
	"time"
)

var (
	latencyPercentiles = []int{50, 60, 70, 80, 90, 95, 99, 100}
	emptyLatencies     = make([]int64, len(latencyPercentiles))
)

// Percentiles
const (
	P50  = iota
	P60  = iota
	P70  = iota
	P80  = iota
	P90  = iota
	P95  = iota
	P99  = iota
	P100 = iota
)

func NewStatsAnalyzer(
	statsCollectors []*StatsCollector,
	opsExecuted *int64,
	latencyChan chan Latency,
	latenciesSize int) *StatsAnalyzer {
	latencies := map[OpType][]int64{}
	lastEndPos := map[OpType]int{}

	for _, opType := range AllOpTypes {
		latencies[opType] = make([]int64, 0, latenciesSize)
		lastEndPos[opType] = 0
	}

	go func() {
		for {
			op, ok := <-latencyChan
			if !ok {
				break
			}
			latencies[op.OpType] = append(
				latencies[op.OpType], int64(op.Latency),
			)
		}
	}()

	return &StatsAnalyzer{
		statsCollectors: statsCollectors,
		opsExecuted:     opsExecuted,
		latencyChan:     latencyChan,
		latencies:       latencies,
		epoch:           time.Now(),
		lastEndPos:      lastEndPos,
	}
}

// ExecutionStatus encapsulates the aggregated information for the execution
type ExecutionStatus struct {
	OpsExecuted        int64
	OpsPerSec          float64
	Duration           time.Duration
	AllTimeLatencies   map[OpType][]int64
	SinceLastLatencies map[OpType][]int64
	Counts             map[OpType]int64
	TypeOpsSec         map[OpType]float64
}

type StatsAnalyzer struct {
	statsCollectors []*StatsCollector
	opsExecuted     *int64
	latencyChan     chan Latency
	latencies       map[OpType][]int64
	epoch           time.Time
	lastEndPos      map[OpType]int
}

func (self *StatsAnalyzer) GetStatus() *ExecutionStatus {
	// Basics
	duration := time.Now().Sub(self.epoch)
	opsPerSec := 0.0
	if duration != 0 {
		opsPerSec = float64(*self.opsExecuted) * float64(time.Second) / float64(duration)
	}

	// Latencies
	stats := CombineStats(self.statsCollectors...)
	allTimeLatencies := make(map[OpType][]int64)
	sinceLastLatencies := make(map[OpType][]int64)
	counts := make(map[OpType]int64)
	typeOpsSec := make(map[OpType]float64)

	for _, opType := range AllOpTypes {
		// take a snapshot of current status since the latency list keeps
		// increasing.
		length := len(self.latencies[opType])
		snapshot := self.latencies[opType][:length]
		lastEndPos := self.lastEndPos[opType]
		self.lastEndPos[opType] = length
		sinceLastLatencies[opType] =
			CalculateLatencyStats(snapshot[lastEndPos:])
		allTimeLatencies[opType] = CalculateLatencyStats(snapshot)
		counts[opType] = stats.Count(opType)
		typeOpsSec[opType] = stats.OpsSec(opType)
	}

	status := ExecutionStatus{
		OpsExecuted:        *self.opsExecuted,
		Duration:           duration,
		OpsPerSec:          opsPerSec,
		AllTimeLatencies:   allTimeLatencies,
		SinceLastLatencies: sinceLastLatencies,
		Counts:             counts,
		TypeOpsSec:         typeOpsSec,
	}

	return &status
}

// Sorting facilities
type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func CalculateLatencyStats(latencies []int64) []int64 {
	result := make([]int64, 0, len(latencyPercentiles))
	length := len(latencies)
	if length == 0 {
		return emptyLatencies
	}
	sort.Sort(int64Slice(latencies))
	for _, perc := range latencyPercentiles {
		result = append(result, latencies[(length-1)*perc/100])
	}
	return result
}
