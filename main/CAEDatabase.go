package main

import (
	"github.com/minio/minio-go/v7"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDB中的数据库对象
type CAEDatabase struct {
	minioC *minio.Client
	Db     *mongo.Database
	Name   string
}

func (caedb *CAEDatabase) Collection(col_name string) (*CAECollection, error) {
	caecol := &CAECollection{}
	var err error = nil

	caecol.minioC = caedb.minioC
	caecol.CollectionObj = caedb.Db.Collection(col_name)
	caecol.Name = col_name

	return caecol, err
}
