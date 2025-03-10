package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/timescale/tsbs/pkg/targets"

	"github.com/valyala/fasthttp"
)

const backingOffChanCap = 100

// allows for testing
var printFn = fmt.Printf

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	httpWriter     *HTTPWriter
}

func (p *processor) Init(numWorker int, _, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	cfg := HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:      daemonURL,
		Database:  loader.DatabaseName(),
	}
	w := NewHTTPWriter(cfg, consistency)
	p.initWithHTTPWriter(numWorker, w)
}

func (p *processor) initWithHTTPWriter(numWorker int, w *HTTPWriter) {
	p.backingOffChan = make(chan bool, backingOffChanCap)
	p.backingOffDone = make(chan struct{})
	p.httpWriter = w
	go p.processBackoffMessages(numWorker)
}

func (p *processor) Close(_ bool) {
	close(p.backingOffChan)
	<-p.backingOffDone
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	log.Println("1")

	// Write the batch: try until backoff is not needed.
	if doLoad {
		log.Println("2")
		var err error
		for {
			log.Println("3")
			if useGzip {
				log.Println("4")
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				log.Println("5")
				fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
				log.Println("6")
				_, err = p.httpWriter.WriteLineProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				log.Println("7")
				compressedBatch.Reset()
				log.Println("8")
				bufPool.Put(compressedBatch)
			} else {
				log.Println("9")
				_, err = p.httpWriter.WriteLineProtocol(batch.buf.Bytes(), false)
			}
			log.Println("10")

			log.Printf("*****err: %s", err)

			if err == errBackoff {
				p.backingOffChan <- true
				time.Sleep(backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	log.Println("11")
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	log.Println("12")
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	log.Println("13")
	return metricCnt, uint64(rowCnt)
}

func (p *processor) processBackoffMessages(workerID int) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range p.backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			printFn("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	printFn("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
	p.backingOffDone <- struct{}{}
}
