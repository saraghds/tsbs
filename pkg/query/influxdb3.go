package query

import (
	"fmt"
	"sync"
)

// InfluxDB3 encodes a InfluxDB3 request. This will be serialized for use
// by the tsbs_run_queries_influxdb3 program.
type InfluxDB3 struct {
	HumanLabel       []byte
	HumanDescription []byte

	SqlQuery []byte
	id       uint64
}

// InfluxDB3Pool is a sync.Pool of InfluxDB3 Query types
var InfluxDB3Pool = sync.Pool{
	New: func() interface{} {
		return &InfluxDB3{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewInfluxDB3 returns a new InfluxDB3 Query instance
func NewInfluxDB3() *InfluxDB3 {
	return InfluxDB3Pool.Get().(*InfluxDB3)
}

// GetID returns the ID of this Query
func (q *InfluxDB3) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *InfluxDB3) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *InfluxDB3) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *InfluxDB3) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *InfluxDB3) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *InfluxDB3) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.SqlQuery = q.SqlQuery[:0]

	InfluxDB3Pool.Put(q)
}
