// 与minio和mongodb的具体操作的函数文件
package main

import (
	"github/medai/mmacess/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func (cae *CAE) connect_to_minio(username, password string) error {
	// 如果需要对username, password处理写道这里
	var err error = nil
	cae.MinioC, err = minio.New(
		config.MINIO_IP+":"+config.MINIO_PORT,
		&minio.Options{
			Creds: credentials.NewStaticV4(
				username, password, ""),
			Secure: false,
		})

	// 中间替换成你自己的连接逻辑
	return err
}
