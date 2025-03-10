package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	httpClientName        = "tsbs_load_influx"
	headerContentEncoding = "Content-Encoding"
	headerGzip            = "gzip"

	maxRetries       = 10
	retryInterval    = 5
	maxRetryInterval = 125
	exponentialBase  = 2
)

var (
	errBackoff          = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b = []byte("i/o timeout")
	backoffMagicWords3  = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  = []byte("timeout")
	backoffMagicWords5  = []byte("write failed: can not exceed max connections of 500")
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig, consistency string) *HTTPWriter {
	url := c.Host + "/write?consistency=" + consistency + "&db=" + url.QueryEscape(c.Database)
	if noSync {
		url += "&no_sync=true"
	}
	fmt.Printf("influx write url: %s\n", url)
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: httpClientName,
		},

		c:   c,
		url: []byte(url),
	}
}

var (
	methodPost = []byte("POST")
	textPlain  = []byte("text/plain")
)

func (w *HTTPWriter) initializeReq(req *fasthttp.Request, body []byte, isGzip bool) {
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(methodPost)
	req.Header.SetRequestURIBytes(w.url)
	if bearer != "" {
		req.Header.Add("Authorization", "Bearer "+bearer)
	} else {
		req.Header.Add("Authorization", "Token "+token)
	}

	if isGzip {
		req.Header.Add(headerContentEncoding, headerGzip)
	}
	req.SetBody(body)
}

func (w *HTTPWriter) executeReq(req *fasthttp.Request, resp *fasthttp.Response) (int64, error) {
	retry := 1
	var lat int64
	var err error
	log.Println("executeReq 1")
	for {
		log.Println("executeReq 2")
		start := time.Now()
		log.Println("executeReq 3")
		err = w.client.Do(req, resp)
		log.Println("executeReq 4")
		log.Printf("executeReq err: %s", err)
		lat = time.Since(start).Nanoseconds()
		if err == nil {
			log.Println("executeReq 5")
			sc := resp.StatusCode()
			log.Printf("executeReq sc: %d", sc)
			fmt.Printf("retry=%d sc=%d lat=%d\n", retry, sc, lat)
			if sc == 500 && backpressurePred(resp.Body()) {
				err = errBackoff
			} else if sc != fasthttp.StatusNoContent {
				err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
			}
		}
		if err == nil || err == errBackoff {
			break
		}
		if retry >= maxRetries {
			break
		}

		log.Println("executeReq 7")
		retryDelay := computeRetryDelay(retry)
		fmt.Printf("retry delay: %dms\n", retryDelay)
		time.Sleep(time.Duration(retryDelay) * time.Millisecond)
		retry++
	}
	return lat, err
}

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip bool) (int64, error) {
	log.Println("WriteLineProtocol 1")
	req := fasthttp.AcquireRequest()
	log.Println("WriteLineProtocol 2")
	defer fasthttp.ReleaseRequest(req)
	log.Println("WriteLineProtocol 3")
	w.initializeReq(req, body, isGzip)

	log.Println("WriteLineProtocol 4")
	resp := fasthttp.AcquireResponse()
	log.Println("WriteLineProtocol 5")
	defer fasthttp.ReleaseResponse(resp)

	log.Println("WriteLineProtocol 6")
	return w.executeReq(req, resp)
}

func backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}

func computeRetryDelay(attempts int) int {
	minDelay := int(retryInterval * math.Pow(exponentialBase, float64(attempts)))
	maxDelay := int(retryInterval * math.Pow(exponentialBase, float64(attempts+1)))
	diff := maxDelay - minDelay
	if diff <= 0 { //check overflows
		return maxRetryInterval
	}
	retryDelay := rand.Intn(diff) + minDelay
	if retryDelay > maxRetryInterval {
		retryDelay = maxRetryInterval
	}
	return retryDelay
}
