package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/timescale/tsbs/pkg/targets"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

const backingOffChanCap = 100

// allows for testing
var printFn = fmt.Printf

type processor struct {
	// backingOffChan chan bool
	// backingOffDone chan struct{}
	client influxdb2.Client
}

func (p *processor) Init(numWorker int, _, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	// cfg := HTTPWriterConfig{
	// 	DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
	// 	Host:      daemonURL,
	// 	Database:  loader.DatabaseName(),
	// }
	// w := NewHTTPWriter(cfg, consistency)
	var c influxdb2.Client
	if bearer != "" {
		httpClient := &http.Client{
			Transport: &CustomTransport{
				Headers: map[string]string{"Authorization": fmt.Sprintf("Bearer %s", bearer)},
			},
		}
		c = influxdb2.NewClientWithOptions(daemonURL, bearer, influxdb2.DefaultOptions().SetHTTPClient(httpClient))
	} else {
		c = influxdb2.NewClientWithOptions(daemonURL, token, influxdb2.DefaultOptions())
	}
	p.initWithClient(numWorker, c)
}

func (p *processor) initWithClient(numWorker int, c influxdb2.Client) {
	// p.backingOffChan = make(chan bool, backingOffChanCap)
	// p.backingOffDone = make(chan struct{})
	p.client = c
	// go p.processBackoffMessages(numWorker)
}

func (p *processor) Close(_ bool) {
	// close(p.backingOffChan)
	// <-p.backingOffDone
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	writeAPI := p.client.WriteAPIBlocking(org, loader.DatabaseName())
	ctx := context.Background()

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		for {
			// if useGzip {
			// 	compressedBatch := bufPool.Get().(*bytes.Buffer)
			// 	fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
			// 	err = writeAPI.WriteRecord(ctx, compressedBatch.String())
			// 	// Return the compressed batch buffer to the pool.
			// 	compressedBatch.Reset()
			// 	bufPool.Put(compressedBatch)
			// } else {
			err = writeAPI.WriteRecord(ctx, batch.buf.String())
			// }

			// if err == errBackoff {
			// 	p.backingOffChan <- true
			// 	time.Sleep(backoff)
			// } else {
			// 	p.backingOffChan <- false
			// 	break
			// }
			if err != nil {
				fatal("Error writing: %s\n", err.Error())
			}
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

// func (p *processor) processBackoffMessages(workerID int) {
// 	var totalBackoffSecs float64
// 	var start time.Time
// 	last := false
// 	for this := range p.backingOffChan {
// 		if this && !last {
// 			start = time.Now()
// 			last = true
// 		} else if !this && last {
// 			took := time.Now().Sub(start)
// 			printFn("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
// 			totalBackoffSecs += took.Seconds()
// 			last = false
// 			start = time.Now()
// 		}
// 	}
// 	printFn("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
// 	p.backingOffDone <- struct{}{}
// }
