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
}

func (d *dbCreator) getClient() *mongo.Client {
	if d.client == nil {
		var err error
		// Use a temporary context for connection establishment
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		defer cancel()
		
		clientOpts := options.Client().ApplyURI(daemonURL).SetConnectTimeout(writeTimeout)
		d.client, err = mongo.Connect(ctx, clientOpts)
		if err != nil {
			log.Fatal(err)
		}
		if err := d.client.Ping(ctx, nil); err != nil {
			log.Fatalf("failed to ping MongoDB: %v", err)
		}
	}
	return d.client
}

func (d *dbCreator) Init() {
	// Initialize connection lazily when needed
	d.getClient()
}

func (d *dbCreator) DBExists(dbName string) bool {
	ctx := context.Background()
	dbs, err := d.getClient().ListDatabaseNames(ctx, bson.M{})
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
	ctx := context.Background()
	collections, err := d.getClient().Database(dbName).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return err
	}
	for _, name := range collections {
		d.getClient().Database(dbName).Collection(name).Drop(ctx)
	}

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	ctx := context.Background()
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

	res := d.getClient().Database(dbName).RunCommand(ctx, cmd, nil)
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create collection err: %v", res.Err())
	}

	collection := d.getClient().Database(dbName).Collection(collectionName)
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
	_, err := collection.Indexes().CreateOne(ctx, index)
	if err != nil {
		return fmt.Errorf("create basic index err: %v", err)
	}

	// To make updates for new records more efficient, we need a efficient doc
	// lookup index
	if !documentPer {
		_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
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
	if d.client != nil {
		ctx := context.Background()
		d.client.Disconnect(ctx)
		d.client = nil
	}
}
