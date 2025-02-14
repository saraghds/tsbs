package main

import "net/http"

// CustomTransport is a custom RoundTripper that adds headers to each request.
type CustomTransport struct {
	Transport http.RoundTripper
	Headers   map[string]string
}

// RoundTrip executes a single HTTP transaction.
func (c *CustomTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Add custom headers to the request
	for key, value := range c.Headers {
		req.Header.Set(key, value)
	}

	// Use the default transport if none is specified
	if c.Transport == nil {
		c.Transport = http.DefaultTransport
	}

	// Perform the request
	return c.Transport.RoundTrip(req)
}
