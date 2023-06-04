package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 这个地方需要你自己写
func (cae *CAE) connect_to_mongodb() error {
	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second)
	defer cancel()

	var err error = nil
	// URI好像就是mongodb的连接入口
	// TODO: 这个地方的mongodb应该修改applyuri
	cae.MongoC, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://192.168.245.130:27017"))
	if err != nil {
		return err
	}

	err = cae.MongoC.Ping(context.Background(), nil)
	return err
}
