package influx

import (
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &influxTarget{}
}

type influxTarget struct {
}

func (t *influxTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Int(flagPrefix+"replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flagSet.String(flagPrefix+"consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
	flagSet.String(flagPrefix+"token", "", "Token to access InfluxDB")
	flagSet.Bool(flagPrefix+"not-create-db", false, "Whether to create the database during test.")
	flagSet.Bool(flagPrefix+"not-drop-db", false, "Whether to drop the database during test.")
	flagSet.String(flagPrefix+"org", "", "InfluxDB org name")
	flagSet.String(flagPrefix+"bearer", "", "Bearer token to access InfluxDB")
	flagSet.Bool(flagPrefix+"no-sync", false, "Whether to disable sync writes (only in InfluxDB 3.x Core and Enterprise)")
}

func (t *influxTarget) TargetName() string {
	return constants.FormatInflux
}

func (t *influxTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *influxTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
