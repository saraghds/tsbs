package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbCreator struct {
	client *mongo.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (d *dbCreator) Init() {
	var err error
	d.ctx, d.cancel = context.WithTimeout(context.Background(), writeTimeout)
	clientOpts := options.Client().ApplyURI(daemonURL).SetConnectTimeout(writeTimeout)
	d.client, err = mongo.Connect(d.ctx, clientOpts)
	if err != nil {
		log.Fatal(err)
	}
	if err := d.client.Ping(d.ctx, nil); err != nil {
		log.Fatalf("failed to ping MongoDB: %v", err)
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.client.ListDatabaseNames(d.ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range dbs {
		if name == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	collections, err := d.client.Database(dbName).ListCollectionNames(d.ctx, bson.M{})
	if err != nil {
		return err
	}
	for _, name := range collections {
		d.client.Database(dbName).Collection(name).Drop(d.ctx)
	}

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.E{Key: "create", Value: collectionName})

	// wiredtiger settings
	cmd = append(cmd, bson.E{
		Key: "storageEngine", Value: map[string]interface{}{
			"wiredTiger": map[string]interface{}{
				"configString": "block_compressor=snappy",
			},
		},
	})

	res := d.client.Database(dbName).RunCommand(d.ctx, cmd, nil)
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create collection err: %v", res.Err())
	}

	collection := d.client.Database(dbName).Collection(collectionName)
	var keys bson.D
	if documentPer {
		keys = bson.D{
			{Key: "measurement", Value: 1},
			{Key: "tags.hostname", Value: 1},
			{Key: timestampField, Value: 1},
		}
	} else {
		keys = bson.D{
			{Key: aggKeyID, Value: 1},
			{Key: "measurement", Value: 1},
			{Key: "tags.hostname", Value: 1},
		}
	}

	index := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(false).SetSparse(false), // Unique does not work on the entire array of tags!
	}
	_, err := collection.Indexes().CreateOne(d.ctx, index)
	if err != nil {
		return fmt.Errorf("create basic index err: %v", err)
	}

	// To make updates for new records more efficient, we need a efficient doc
	// lookup index
	if !documentPer {
		_, err := collection.Indexes().CreateOne(d.ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: aggDocID, Value: 1}},
			Options: options.Index().SetUnique(false).SetSparse(false),
		})
		if err != nil {
			return fmt.Errorf("create agg doc index err: %v", err)
		}
	}

	return nil
}

func (d *dbCreator) Close() {
	d.client.Disconnect(d.ctx)
	d.cancel()
}
