package flashback

import (
	"github.com/bmizerany/perks/quantile"
	"sync"
	"time"
)

type OpStat struct {
	OpType  OpType
	Latency time.Duration
	OpError bool
}

var (
	latencyPercentiles = []float64{0.5, 0.7, 0.9, 0.95, 0.99}
)

// Percentiles
const (
	P50 = iota
	P70 = iota
	P90 = iota
	P95 = iota
	P99 = iota
)

type StatsAnalyzer struct {
	statsChan chan OpStat

	startTime   time.Time
	stream      map[OpType]*quantile.Stream
	maxLatency  map[OpType]float64
	opsExecuted int64
	opsErrors   int64
	counts      map[OpType]int64

	intervalStartTime   time.Time
	intervalStream      map[OpType]*quantile.Stream
	intervalMaxLatency  map[OpType]float64
	intervalOpsExecuted int64
	intervalOpsErrors   int64
	intervalCounts      map[OpType]int64

	mutex *sync.Mutex
}

func (s *StatsAnalyzer) process(opStat OpStat) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.counts[opStat.OpType]++
	s.intervalCounts[opStat.OpType]++
	s.opsExecuted++
	s.intervalOpsExecuted++
	if opStat.OpError == true {
		s.opsErrors++
		s.intervalOpsErrors++
	}

	latencyMs := float64(opStat.Latency) / float64(time.Millisecond)
	s.stream[opStat.OpType].Insert(latencyMs)
	s.intervalStream[opStat.OpType].Insert(latencyMs)

	if s.maxLatency[opStat.OpType] < latencyMs {
		s.maxLatency[opStat.OpType] = latencyMs
	}
	if s.intervalMaxLatency[opStat.OpType] < latencyMs {
		s.intervalMaxLatency[opStat.OpType] = latencyMs
	}
}

func NewStatsAnalyzer(statsChan chan OpStat) *StatsAnalyzer {
	stream := make(map[OpType]*quantile.Stream)
	intervalStream := make(map[OpType]*quantile.Stream)
	for _, opType := range AllOpTypes {
		stream[opType] = quantile.NewTargeted(0.5, 0.7, 0.9, 0.95, 0.99)
		intervalStream[opType] = quantile.NewTargeted(0.5, 0.7, 0.9, 0.95, 0.99)
	}
	statsAnalyzer := &StatsAnalyzer{
		statsChan:           statsChan,
		startTime:           time.Now(),
		stream:              stream,
		maxLatency:          make(map[OpType]float64),
		opsExecuted:         0,
		opsErrors:           0,
		counts:              make(map[OpType]int64),
		intervalStartTime:   time.Now(),
		intervalStream:      intervalStream,
		intervalMaxLatency:  make(map[OpType]float64),
		intervalOpsExecuted: 0,
		intervalOpsErrors:   0,
		intervalCounts:      make(map[OpType]int64),
		mutex:               &sync.Mutex{},
	}

	go func() {
		for {
			op, ok := <-statsAnalyzer.statsChan
			if !ok {
				break
			}
			statsAnalyzer.process(op)
		}
	}()

	return statsAnalyzer
}

// ExecutionStatus encapsulates the aggregated information for the execution
type ExecutionStatus struct {
	OpsExecuted         int64
	IntervalOpsExecuted int64
	OpsErrors           int64
	IntervalOpsErrors   int64
	OpsPerSec           float64
	IntervalOpsPerSec   float64
	IntervalDuration    time.Duration
	Latencies           map[OpType][]float64
	IntervalLatencies   map[OpType][]float64
	MaxLatency          map[OpType]float64
	IntervalMaxLatency  map[OpType]float64
	Counts              map[OpType]int64
	IntervalCounts      map[OpType]int64
	TypeOpsSec          map[OpType]float64
	IntervalTypeOpsSec  map[OpType]float64
}

func (s *StatsAnalyzer) GetStatus() *ExecutionStatus {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	opsExecuted := s.opsExecuted
	intervalOpsExecuted := s.intervalOpsExecuted
	opsErrors := s.opsErrors
	intervalOpsErrors := s.intervalOpsErrors

	now := time.Now()
	durationSec := float64(now.Sub(s.startTime)) / float64(time.Second)
	opsPerSec := float64(opsExecuted) / durationSec
	intervalDuration := now.Sub(s.intervalStartTime)
	intervalDurationSec := float64(intervalDuration) / float64(time.Second)
	intervalOpsPerSec := float64(intervalOpsExecuted) / intervalDurationSec

	latencies := make(map[OpType][]float64)
	intervalLatencies := make(map[OpType][]float64)
	counts := make(map[OpType]int64)
	intervalCounts := make(map[OpType]int64)
	typeOpsSec := make(map[OpType]float64)
	intervalTypeOpsSec := make(map[OpType]float64)
	maxLatency := make(map[OpType]float64)
	intervalMaxLatency := make(map[OpType]float64)

	for _, opType := range AllOpTypes {
		maxLatency[opType] = s.maxLatency[opType]
		intervalMaxLatency[opType] = s.intervalMaxLatency[opType]
		for _, percentile := range latencyPercentiles {
			latencies[opType] = append(latencies[opType], s.stream[opType].Query(percentile))
			intervalLatencies[opType] = append(intervalLatencies[opType],
				s.intervalStream[opType].Query(percentile))
		}
		counts[opType] = s.counts[opType]
		intervalCounts[opType] = s.intervalCounts[opType]

		typeOpsSec[opType] = float64(s.counts[opType]) / durationSec
		intervalTypeOpsSec[opType] = float64(s.intervalCounts[opType]) / intervalDurationSec
	}

	status := ExecutionStatus{
		OpsExecuted:         opsExecuted,
		IntervalOpsExecuted: intervalOpsExecuted,
		OpsErrors:           opsErrors,
		IntervalOpsErrors:   intervalOpsErrors,
		OpsPerSec:           opsPerSec,
		IntervalOpsPerSec:   intervalOpsPerSec,
		IntervalDuration:    intervalDuration,
		Latencies:           latencies,
		IntervalLatencies:   intervalLatencies,
		MaxLatency:          maxLatency,
		IntervalMaxLatency:  intervalMaxLatency,
		Counts:              counts,
		IntervalCounts:      intervalCounts,
		TypeOpsSec:          typeOpsSec,
		IntervalTypeOpsSec:  intervalTypeOpsSec,
	}

	// reset interval
	s.intervalStartTime = now
	for _, opType := range AllOpTypes {
		s.intervalStream[opType].Reset()
		s.intervalCounts[opType] = 0
		s.intervalMaxLatency[opType] = 0
	}
	s.intervalOpsExecuted = 0
	s.intervalOpsErrors = 0

	return &status
}
