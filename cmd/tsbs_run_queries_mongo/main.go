// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mongo-driver.
package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/blagojts/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonURL string
	timeout   time.Duration
)

// Global vars:
var (
	runner *query.BenchmarkRunner
	client *mongo.Client
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "mongodb://localhost:27017", "Daemon URL.")
	pflag.Duration("read-timeout", 30*time.Second, "Timeout value for individual queries")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	daemonURL = viper.GetString("url")
	timeout = viper.GetDuration("read-timeout")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	clientOpts := options.Client().ApplyURI(daemonURL).SetConnectTimeout(timeout)
	var err error
	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("failed to ping MongoDB: %v", err)
	}

	runner.Run(&query.MongoPool, newProcessor)
}

type processor struct {
	collection *mongo.Collection
	ctx        context.Context
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.ctx = context.Background()
	p.collection = client.Database(runner.DatabaseName()).Collection("point_data")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()
	
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := p.collection.Aggregate(p.ctx, mq.BsonDoc, opts)
	if err != nil {
		took := time.Now().UnixNano() - start
		lag := float64(took) / 1e6 // milliseconds
		stat := query.GetStat()
		stat.Init(q.HumanLabelName(), lag)
		return []*query.Stat{stat}, err
	}
	defer cursor.Close(p.ctx)
	
	if runner.DebugLevel() > 0 {
		fmt.Println(mq.BsonDoc)
	}
	
	var result map[string]interface{}
	cnt := 0
	for cursor.Next(p.ctx) {
		if err := cursor.Decode(&result); err != nil {
			took := time.Now().UnixNano() - start
			lag := float64(took) / 1e6 // milliseconds
			stat := query.GetStat()
			stat.Init(q.HumanLabelName(), lag)
			return []*query.Stat{stat}, err
		}
		if runner.DoPrintResponses() {
			fmt.Printf("ID %d: %v\n", q.GetID(), result)
		}
		cnt++
	}
	if runner.DebugLevel() > 0 {
		fmt.Println(cnt)
	}
	
	if err := cursor.Err(); err != nil {
		took := time.Now().UnixNano() - start
		lag := float64(took) / 1e6 // milliseconds
		stat := query.GetStat()
		stat.Init(q.HumanLabelName(), lag)
		return []*query.Stat{stat}, err
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
