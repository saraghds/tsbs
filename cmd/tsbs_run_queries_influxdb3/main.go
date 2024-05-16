// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Program option vars:
var (
	host          string
	port          string
	token         string
	bucket        string
	database      string
	secure        bool
	flightSQL     bool
	printResponse bool
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
	pflag.Bool("flightsql", false, "Using FlightSQL")

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
	flightSQL = viper.GetBool("flightsql")

	runner = query.NewBenchmarkRunner(config)
	printResponse = runner.DoPrintResponses()
}

func main() {
	runner.Run(&query.InfluxDB3Pool, newProcessor)
}

type processor struct {
	client          *influxdb3.Client
	flightSqlClient *flightsql.Client
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	hostPort := fmt.Sprintf("%s:%s", host, port)

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     hostPort,
		Token:    token,
		Database: database,
	})
	databases.PanicIfErr(err)
	p.client = client

	hostPort = strings.Replace(hostPort, "http://", "", 1)
	hostPort = strings.Replace(hostPort, "https://", "", 1)

	var dialOpt grpc.DialOption
	if secure {
		clientCreds := credentials.NewTLS(&tls.Config{})
		dialOpt = grpc.WithTransportCredentials(clientCreds)
	} else {
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	flightSqlClient, err := flightsql.NewClient(hostPort, nil, nil, dialOpt)
	databases.PanicIfErr(err)
	p.flightSqlClient = flightSqlClient
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.InfluxDB3)
	start := time.Now()
	qry := string(tq.SqlQuery)

	if flightSQL {
		ctx := context.Background()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Token %s", token))
		if bucket != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", bucket)
		}
		if database != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "database", database)
		}
		flightInfo, err := p.flightSqlClient.Execute(ctx, qry)
		databases.PanicIfErr(err)

		if printResponse {
			output := ""
			for _, endpoint := range flightInfo.Endpoint {
				flightReader, err := p.flightSqlClient.DoGet(ctx, endpoint.Ticket)
				databases.PanicIfErr(err)
				for flightReader.Next() {
					record := flightReader.Record()
					output += fmt.Sprintf("%v\n", record)
				}
				flightReader.Release()
			}

			fmt.Printf("%s\n\n%s\n-----\n\n", qry, output)
		} else {
			for _, endpoint := range flightInfo.Endpoint {
				flightReader, err := p.flightSqlClient.DoGet(ctx, endpoint.Ticket)
				databases.PanicIfErr(err)
				// Fetching all the rows to confirm that the query is fully completed.
				for flightReader.Next() {
				}
				flightReader.Release()
			}
		}
	} else {
		iterator, err := p.client.Query(context.Background(), qry)
		databases.PanicIfErr(err)

		if printResponse {
			output := ""
			for iterator.Next() {
				value := iterator.Value()
				output += fmt.Sprintf("%s\n", fmt.Sprint(value))
			}
			fmt.Printf("%s\n\n%s\n-----\n\n", qry, output)
		} else {
			// Fetching all the rows to confirm that the query is fully completed.
			for iterator.Next() {
			}
		}
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, nil
}
