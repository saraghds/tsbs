package load

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

type noFlowBenchmarkRunner struct {
	CommonBenchmarkRunner
}

func (l *noFlowBenchmarkRunner) RunBenchmark(b targets.Benchmark) {
	fmt.Println("noFlowBenchmarkRunner RunBenchmark")
	wg, start := l.preRun(b)

	var numChannels uint
	if l.HashWorkers {
		numChannels = l.Workers
	} else {
		numChannels = 1
	}
	fmt.Println("noFlowBenchmarkRunner RunBenchmark 1")
	channels := l.createChannels(numChannels, l.ChannelCapacity)

	// Launch all worker processes in background
	fmt.Println("noFlowBenchmarkRunner RunBenchmark 2")
	for i := uint(0); i < l.Workers; i++ {
		go l.work(b, wg, channels[i%numChannels], i)
	}
	// Start scan process - actual data read process
	fmt.Println("noFlowBenchmarkRunner RunBenchmark 3")
	scanWithoutFlowControl(b.GetDataSource(), b.GetPointIndexer(numChannels), b.GetBatchFactory(), channels, l.BatchSize, l.Limit)
	for _, c := range channels {
		close(c)
	}
	fmt.Println("noFlowBenchmarkRunner RunBenchmark 4")
	l.postRun(wg, start)
	fmt.Println("noFlowBenchmarkRunner RunBenchmark 5")
}

// createChannels create channels from which workers would receive tasks
// One channel per worker
func (l *noFlowBenchmarkRunner) createChannels(numChannels, capacity uint) []chan targets.Batch {
	// Result - channels to be created
	channels := make([]chan targets.Batch, numChannels)
	for i := uint(0); i < numChannels; i++ {
		channels[i] = make(chan targets.Batch, capacity)
	}
	return channels
}

// work is the processing function for each worker in the loader
func (l *noFlowBenchmarkRunner) work(b targets.Benchmark, wg *sync.WaitGroup, c <-chan targets.Batch, workerNum uint) {
	fmt.Println("noFlowBenchmarkRunner work")
	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(int(workerNum), l.DoLoad, l.HashWorkers)
	fmt.Println("noFlowBenchmarkRunner work 1")

	// Process batches coming from the incoming queue (c)
	for batch := range c {
		fmt.Println("noFlowBenchmarkRunner work 2")
		startedWorkAt := time.Now()
		fmt.Println("noFlowBenchmarkRunner work 3")
		metricCnt, rowCnt := proc.ProcessBatch(batch, l.DoLoad)
		fmt.Println("noFlowBenchmarkRunner work 4")
		atomic.AddUint64(&l.metricCnt, metricCnt)
		fmt.Println("noFlowBenchmarkRunner work 5")
		atomic.AddUint64(&l.rowCnt, rowCnt)
		fmt.Println("noFlowBenchmarkRunner work 6")
		l.timeToSleep(workerNum, startedWorkAt)
	}

	// Close proc if necessary
	fmt.Println("noFlowBenchmarkRunner work 7")
	switch c := proc.(type) {
	case targets.ProcessorCloser:
		c.Close(l.DoLoad)
	}

	fmt.Println("noFlowBenchmarkRunner work 8")
	wg.Done()
}
