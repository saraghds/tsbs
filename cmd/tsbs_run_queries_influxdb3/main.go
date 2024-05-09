// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	host     string
	port     string
	token    string
	bucket   string
	database string
	secure   bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Host")
	pflag.String("port", "81", "Port")
	pflag.String("token", "token", "Token")
	pflag.String("bucket", "", "Bucket name for serverless")
	pflag.String("database", "", "Database name for dedicated")
	pflag.Bool("secure", false, "Secure transport credentials")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host = viper.GetString("host")
	port = viper.GetString("port")
	token = viper.GetString("token")
	bucket = viper.GetString("bucket")
	database = viper.GetString("database")
	secure = viper.GetBool("secure")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.InfluxDB3Pool, newProcessor)
}

type processor struct {
	client *influxdb3.Client
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     fmt.Sprintf("%s:%s", host, port),
		Token:    token,
		Database: database,
	})
	databases.PanicIfErr(err)
	p.client = client
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	// tq := q.(*query.InfluxDB3)
	start := time.Now()
	qry := "show tables"

	iterator, err := p.client.Query(context.Background(), qry)
	databases.PanicIfErr(err)

	output := ""
	for iterator.Next() {
		value := iterator.Value()
		output += fmt.Sprintf("%s\n", fmt.Sprint(value))
	}
	fmt.Printf("%s\n\n%s\n-----\n\n", qry, output)

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, nil
}
