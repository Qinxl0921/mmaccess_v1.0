package main

import (
	"fmt"
	"github/medai/mmacess/config"
	"testing"
)

func TestDeferScope(t *testing.T) {
	fmt.Println("1111111111111111")
	defer fmt.Println("222222222222222")
	fmt.Println("3333333333333333333")
	defer fmt.Println("44444444444444444444")
	fmt.Println("55555555555555555")
	// 结果是13542，说明{}不能构成一个单独的代码块
	// 测试循环内部使用defer
	nums := []int{1, 2, 3}
	for _, num := range nums {
		defer func(a int) {
			fmt.Println(a)
		}(num)
	}
}

func TestFind_One(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)
	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)

	filter := Filter{"_id": "0005"}
	pro := MakeProjection("Speed", "deck", "TrailName")
	fmt.Println(colobj.FindOne(filter, pro))
	fmt.Println(colobj.Find(filter, pro))
	filter = Filter{"name": "zsq"}
	pro = MakeProjection()
	fmt.Println(colobj.FindOne(filter, pro))
	fmt.Println(colobj.Find(filter, pro))

	fmt.Println("测试结束")
}

func TestDeleteData(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)
	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)

	fmt.Println(colobj.DeleteData("0004", "ceshi"))
	fmt.Println(colobj.DeleteTrail("0004"))

	fmt.Println("测试结束")
}

func TestGetTrail(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)
	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)

	fmt.Println(colobj.GetTrail("0005", "./get0005"))

	fmt.Println("测试结束")
}

func TestJSONFileOp(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)
	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)

	err = colobj.PushByJSON("0005", "../test.json")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("测试结束")
}

func TestFileOp(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)
	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)

	err = colobj.PushFile("0004", "ceshi", "../ceshi.txt")
	if err != nil {
		fmt.Println(err)
	}

	err = colobj.GetFile("0004", "ceshi", "ceshi.txt")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("测试结束")
}

func TestCAE(t *testing.T) {
	cae := New(config.MINIO_IP, config.MINIO_PORT, config.MONGODBIP, config.MONGODBPORT)

	err := cae.Connect("minioadmin", "minioadmin")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建连接")

	defer cae.Close()

	dbobj, err := cae.Database("my_db", true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功创建dbobj")
	colobj, err := dbobj.Collection("test1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("成功拿到表")
	fmt.Println(colobj)
	err = colobj.PushData("0001", "age", 33)
	if err != nil {
		fmt.Printf("推入数据时出错:%v", err)
		// return
	} // 这个trailId是唯一的，如果存在则更新，不存在则创建

	fmt.Println("查询某一个属性")
	if res, err := colobj.GetData("0001", "name"); err != nil {
		fmt.Println("出错了：")
		fmt.Println(err)
		return
	} else {
		fmt.Printf("%s\n", res)
	}
	fmt.Println("准备查询空串")
	if res, err := colobj.GetData("0001", ""); err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Println(res)
	}
	fmt.Println("准备查询不存在trailid")
	if res, err := colobj.GetData("0004", ""); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}
	fmt.Println("准备查询不存在paramname")
	if res, err := colobj.GetData("0001", "aaaaa"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res["aaaaa"])
	}

	fmt.Println("成功测试")
}
