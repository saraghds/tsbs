package influxdb3

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

// BaseGenerator contains settings specific for Influx database.
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.InfluxDB3.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewInfluxDB3()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, table, sql string) {
	q := qi.(*query.InfluxDB3)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
