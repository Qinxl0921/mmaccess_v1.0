// CAE.go
// 一些外放的接口，在这里写
package main

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// 连接minio和mongodb的句柄
type CAE struct {
	MinioC *minio.Client
	MongoC *mongo.Client

	MINIOIP     string
	MINIOPORT   string
	MONGODBIP   string
	MONGODBPORT string
}

// 创建一个CAE对象
func New(MINIOIP, MINIOPORT, MONGODBIP, MONGODBPORT string) *CAE {
	return &CAE{
		MINIOIP:     MINIOIP,
		MINIOPORT:   MINIOPORT,
		MONGODBIP:   MONGODBIP,
		MONGODBPORT: MONGODBPORT,
	}
}

// 这种方法是先创建一个CAE对象，然后利用对象的连接方法进行连接
// 连接到minio和mongodb的服务，分别填写他们的账号密码
func (cae *CAE) Connect(minio_username, minio_password string) error {
	var err error = nil

	err = cae.connect_to_minio(minio_username, minio_password)
	if err != nil {
		return fmt.Errorf("连接minio出错：%v", err)
	}

	err = cae.connect_to_mongodb()
	if err != nil {
		return fmt.Errorf("连接mongo出错：%v", err)
	}
	return nil
}

func (cae *CAE) Close() error {
	// TODO: 好像MinioC不需要关闭，这个地方后续解决或者确认

	err := cae.MongoC.Disconnect(context.Background())
	if err != nil {
		return fmt.Errorf("退出mongo连接出错: %v", err)
	}
	return nil
}

func (cae *CAE) isDBExists(dbname string) (bool, error) {
	dbnames, err := cae.MongoC.ListDatabaseNames(
		context.Background(), bson.D{})
	if err != nil {
		return false, fmt.Errorf("获取数据库名字列表时出错：%v", err)
	}
	for _, str := range dbnames {
		if dbname == str {
			// 说明存在，可以break
			return true, nil
		}
	} // 走到这里，要么创建好了，要么是存在的
	return false, nil
}

// 我猜测这里应该是一个mongodb的代码，mongodb中好像有这个东西
// dbname是获取这个数据库的句柄，create指定如果不存在是否创建这个数据库
func (cae *CAE) Database(dbname string, create bool) (*CAEDatabase, error) {
	// 首先需要判断这个数据库是否存在
	if flag, err := cae.isDBExists(dbname); err != nil || !flag {
		if err != nil {
			return nil, err
		}
		if !flag {
			if !create {
				return nil, fmt.Errorf("数据库不存在")
			}
		} // 不存在且不需要创建的时候，就需要返回了
	}

	// 这里，如果不存在会创建，然后返回数据库的句柄
	caedb := &CAEDatabase{
		cae.MinioC,
		cae.MongoC.Database(dbname),
		dbname,
	}

	return caedb, nil
}
