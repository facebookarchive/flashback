package flashback

import (
	"time"
)

func NewBestEffortOpsDispatcher(reader OpsReader, opsSize int, logger *Logger) chan *Op {
	queue := make([]*Op, opsSize, opsSize)
	i := 0

	// preload all the ops to avoid any overhead for fetching ops.
	logger.Info("Started preloading ops: as fast as possible")
	epoch := time.Now()
	reportStatus := func() {
		logger.Infof("%d ops loaded, %.2f ops/sec\n", reader.OpsRead(),
			float64(reader.OpsRead())/time.Now().Sub(epoch).Seconds())
	}
	defer reportStatus()

	for ; i < opsSize && !reader.AllLoaded(); i++ {
		op := reader.Next()
		if op == nil {
			break
		}
		queue[i] = op

		if i != 0 && i%30000 == 0 {
			reportStatus()
		}
	}
	opChannel := make(chan *Op, 10000)
	// start a gorountine to dispatch these ops as fast as workers can handle.
	go func() {
		logger.Info("Started dispatching ops: as fast as possible")
		for i, op := range queue {
			queue[i] = nil
			opChannel <- op
		}
		close(opChannel)
		logger.Info("Dispatching ended")
	}()

	return opChannel
}

func NewByTimeOpsDispatcher(reader OpsReader, opsSize int, logger *Logger) chan *Op {
	opChannel := make(chan *Op, 5000)
	go func() {
		logger.Info("Started replaying ops by time")
		now_epoch := time.Now()
		epoch := time.Unix(0, 0)
		for i := 0; i < opsSize && !reader.AllLoaded(); i++ {
			op := reader.Next()
			if op == nil {
				break
			}
			if epoch.Unix() == 0 {
				epoch = op.Timestamp
			}

			elapsed := op.Timestamp.Sub(epoch)
			currentClapsed := time.Now().Sub(now_epoch)
			if elapsed > currentClapsed {
				time.Sleep(elapsed - currentClapsed)
			}
			opChannel <- op
			if reader.OpsRead()%10000 == 0 {
				logger.Info("Timestamp for latest op: ", op.Timestamp)
			}
		}
		logger.Info("Dispatching ended")
		close(opChannel)
	}()
	return opChannel
}
