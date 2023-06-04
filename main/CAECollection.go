package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CAECollection struct {
	minioC        *minio.Client
	CollectionObj *mongo.Collection
	Name          string
}

type Filter bson.M
type Projection bson.D

// 插入一条数据，trailID是唯一的，如果存在则更新，如果不存在则创建
func (caecol *CAECollection) PushData(trailId, paramName string, value interface{}) error {
	//TODO: 可以使用type获取类型，进而进一步处理，这里没有处理细节，先不管
	switch value.(type) {
	case int:
		// fmt.Println("Interger")
	case string:
		// fmt.Println("string")
	case float64:
		// fmt.Println("double")
	case float32:
		// fmt.Println("float")
	default:
		return fmt.Errorf("value的类型应该是其中之一：int, string, float64, float32")
	}

	if _, err := caecol.CollectionObj.UpdateOne(
		context.Background(),
		bson.D{{"_id", trailId}},
		bson.D{{"$set", bson.D{{paramName, value}}}},
		options.Update().SetUpsert(true)); err != nil {
		// 手动对数据设置一个id，id对应住paramName，然后对应value
		return fmt.Errorf("插入数据trailId: %v时出错：%v", trailId, err)
	}

	return nil
}

// 更新
func (caecol *CAECollection) UpdateData(trailId, paramName string, paramValue interface{}) error {
	return caecol.PushData(trailId, paramName, paramValue)
}

// 获取一条数据，根据trailId和paramName获取
// 如果paramName是空字符串 "" 则查询trailId下的所有内容，否则按照指定的paramName查询
func (caecol *CAECollection) GetData(trailId, paramName string) (bson.M, error) {
	var res bson.M
	var opts = options.FindOne()
	if paramName == "" { // 这个地方应该是查询到所有的param和value
		opts = options.FindOne()
	} else {
		opts = options.FindOne().SetProjection(bson.M{paramName: 1})
	}

	err := caecol.CollectionObj.FindOne(
		context.TODO(),
		bson.M{"_id": trailId},
		opts).Decode(&res)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return bson.M{}, nil
		}
		return nil, fmt.Errorf("查找数据时错误:%v", err)
	}
	return res, nil
}

// 上传一个文件，在mongodb中存的是：
// {"_id": trailId, filename: fileName_file}
// 在minio中存放的是fileName_file: file，存放的桶名字应该是trailId
func (caecol *CAECollection) PushFile(trailId, fileName, filePath string) error {

	// 可以通过collection获得database，但是怎么获得minio的句柄，所以这个参数应该加一个miniocslient
	// 首先上传文件，如果文件无法上传，一切别说

	//在文件开始创建之前进度条状态=true
	// startTime := time.Now().String()
	// startTime_int64 := time.Now().Unix()
	// startTime := strconv.FormatInt(startTime_int64, 10)
	// sendfristinfo := "trailId=" + trailId + "&fileName=" + fileName + "&status=true" + "&startTime=" + time.Now().String() + "&endTime=0"
	// if _, err := http.Get("http://localhost:8080/websocket?" + sendfristinfo); err != nil {
	// 	fmt.Print("已发送true=====")
	// 	fmt.Println("没有连接dbm")
	// }

	if flag, err := caecol.minioC.BucketExists(context.Background(), trailId); err != nil {
		return err
	} else {
		if !flag {
			// 创建一个桶
			fmt.Println("创建桶")
			err = caecol.minioC.MakeBucket(context.Background(), trailId, minio.MakeBucketOptions{})
			if err != nil {
				return err
			}
		}
	} // 判断桶是否存在，不存在创建桶
	// 使用trailId标明桶的名字，方便两边互相对照

	// 这样取的时候也方便，直接拼接路径即可
	fileNameInMinIO := "file/" + filepath.Base(filePath)
	// info, err := minioClient.FPutObject(
	_, err := caecol.minioC.FPutObject(
		context.Background(),
		trailId,         // bucketName
		fileNameInMinIO, //objectname，前面加一个_file，用于指定是file
		filePath,        // 文件路径
		minio.PutObjectOptions{},
	)
	if err != nil {
		if err.Error() == "open "+filePath+": The system cannot find the file specified." {
			cwd, _ := os.Getwd()
			fmt.Println("文件找不到，检查路径，当前工作路径是：", cwd)
		}
		return err
	}
	// fmt.Println("info: ", info)

	// 然后把关键字信息存入mongodb中
	// 现在minio中，trailid的桶中，文件存放的名字是filename
	// 在mongodb中，应该在traiId的行中，存放{fileName: 存在minio中的filename}
	/*	err = caecol.PushData(trailId, fileName, fileNameInMinIO)

		if err != nil {
			fmt.Println("!!!上传文件已经成功，但是存放上传信息时出错，可重新执行下面命令上传信息：")
			fmt.Println("caecol.PushData(", trailId, ", \""+fileName+"\", \""+fileNameInMinIO+"\")")
			return err
		}
	*/

	// 向后端发送上传文件信息
	sendendinfo := "trailId=" + trailId + "&fileName=" + fileName
	if _, err := http.Get("http://localhost:8080/websocket?" + sendendinfo); err != nil {
		fmt.Println("信息已发送=====")
		fmt.Println("没有连接dbm")
	}
	fmt.Print("=====上传完成=====")
	return nil
}
func (caecol *CAECollection) PushFilebyJson(trailId, fileName, filePath string) error {

	// 可以通过collection获得database，但是怎么获得minio的句柄，所以这个参数应该加一个miniocslient
	// 首先上传文件，如果文件无法上传，一切别说

	//在文件开始创建之前进度条状态=true

	if flag, err := caecol.minioC.BucketExists(context.Background(), trailId); err != nil {
		return err
	} else {
		if !flag {
			// 创建一个桶

			err = caecol.minioC.MakeBucket(context.Background(), trailId, minio.MakeBucketOptions{})
			if err != nil {
				return err
			}
		}
	} // 判断桶是否存在，不存在创建桶
	// 使用trailId标明桶的名字，方便两边互相对照

	// 这样取的时候也方便，直接拼接路径即可
	fileNameInMinIO := "file/" + filepath.Base(filePath)
	// info, err := minioClient.FPutObject(
	_, err := caecol.minioC.FPutObject(
		context.Background(),
		trailId,         // bucketName
		fileNameInMinIO, //objectname，前面加一个_file，用于指定是file
		filePath,        // 文件路径
		minio.PutObjectOptions{},
	)
	if err != nil {
		if err.Error() == "open "+filePath+": The system cannot find the file specified." {
			cwd, _ := os.Getwd()
			fmt.Println("文件找不到，检查路径，当前工作路径是：", cwd)
		}
		return err
	}
	// fmt.Println("info: ", info)

	// 然后把关键字信息存入mongodb中
	// 现在minio中，trailid的桶中，文件存放的名字是filename
	// 在mongodb中，应该在traiId的行中，存放{fileName: 存在minio中的filename}
	err = caecol.PushData(trailId, fileName, fileNameInMinIO)

	if err != nil {
		fmt.Println("!!!上传文件已经成功，但是存放上传信息时出错，可重新执行下面命令上传信息：")
		fmt.Println("caecol.PushData(", trailId, ", \""+fileName+"\", \""+fileNameInMinIO+"\")")
		return err
	}

	return nil
}

// 更新文件，由于上传文件时，重复了自动更新，所以直接调用即可
func (caecol *CAECollection) UpdateFile(trailId, fileName, filePath string) error {
	return caecol.PushFile(trailId, fileName, filePath)
}

// trailId是实验的ID，fileName是在mongodb中保存的文件名字，在mongodb中拿到在minio中的名字，
// 因为这个在minio中的名字可能会改变，但是这个键肯定不会变
// filePath是文件下载的路径，这个路径应包含最后保存的文件名字
func (caecol *CAECollection) GetFile(trailId, fileName, filePath string) error {
	res, err := caecol.GetData(trailId, fileName)
	if err != nil {
		return err
	}
	fileNameInMinIO, ok := res[fileName].(string)
	if !ok {
		// 类型不对
		return fmt.Errorf("fileName应该是字符串类型，请检查mongodb中的数据")
	}
	err = caecol.minioC.FGetObject(
		context.Background(),
		trailId,         // bucketName
		fileNameInMinIO, // objectName，就是MinIO中的名字
		filePath,        // 存放文件的路径，保存的位置
		minio.GetObjectOptions{},
	)
	if err != nil {
		return err
	}

	return nil
}

func isFile(name string) bool {
	// 判断json中的某个value是不是文件，因为文件都有后缀，如.stl，所以直接判断.的存在即可。
	return strings.Contains(name, "./") || strings.Contains(name, "/") || strings.Contains(name, ".")
}

// 通过一个json文件上传
func (caecol *CAECollection) PushByJSON(trailId, jsonPath string) error {

	sendfristinfo := "trailId=" + trailId + "&status=true"
	if _, err := http.Get("http://localhost:8088/programbar?" + sendfristinfo); err != nil {
		return err
	}

	// 首先读取JSON文件
	jsonFile, err := os.Open(jsonPath)
	if err != nil {
		return nil
	}
	defer jsonFile.Close()
	jsonData := make(map[string]interface{})
	if err := json.NewDecoder(jsonFile).Decode(&jsonData); err != nil {
		return err
	}

	// 然后对于各个值，进行判断
	for paramName, paramValue := range jsonData {
		fmt.Println(paramName, paramValue)
		switch pv := paramValue.(type) {
		case int:
			// fmt.Println(paramValue, "是int型")
			err := caecol.PushData(trailId, paramName, pv) // 插入这个数据
			// 由于循环是改变的，所以这个地方使用匿名函数重新传递变量插入。但是trailId是没有改变的，所以没必要传递一下了
			if err != nil {
				return err
			} else {
				fmt.Printf("插入数据:%v, %v成功\n", paramName, pv)
			}
		case float32:
			// fmt.Println(paramValue, "是浮点型")
			err := caecol.PushData(trailId, paramName, pv) // 插入这个数据
			// 由于循环是改变的，所以这个地方使用匿名函数重新传递变量插入。但是trailId是没有改变的，所以没必要传递一下了
			if err != nil {
				return err
			} else {
				fmt.Printf("插入数据:%v, %v成功\n", paramName, pv)
			}
			// 在type的判断中不能fallthrough
		case float64:
			// fmt.Println(paramValue, "是浮点型")
			err := caecol.PushData(trailId, paramName, pv) // 插入这个数据
			// 由于循环是改变的，所以这个地方使用匿名函数重新传递变量插入。但是trailId是没有改变的，所以没必要传递一下了
			if err != nil {
				return err
			} else {
				fmt.Printf("插入数据:%v, %v成功\n", paramName, pv)
			}
		case string:
			// 这个地方要判断paramValue是普通字符串还是文件了，如果是普通字符串，直接插入，如果是文件，上传再插入
			// fmt.Println(paramValue, "是字符串")
			if isFile(pv) {
				err := caecol.PushFilebyJson(trailId, paramName, pv)
				if err != nil {
					return err
				} else {
					fmt.Printf("插入数据:%v, %v成功\n", paramName, pv)
				}
			} else {
				err := caecol.PushData(trailId, paramName, pv)
				if err != nil {
					return err
				} else {
					fmt.Printf("插入数据:%v, %v成功\n", paramName, pv)
				}
			}
		default:
			return fmt.Errorf("%v不支持的数据类型", paramValue)
		}

	}

	sendendinfo := "trailId=" + trailId + "&status=false"
	if _, err := http.Get("http://localhost:8088/programbar?" + sendendinfo); err != nil {
		return err
	}

	return nil
}

// 通过json更新，同样直接调用push
func (caecol *CAECollection) UpdateByJson(trailId, jsonPath string) error {
	return caecol.PushByJSON(trailId, jsonPath)
}

// 读取trailId中的所有内容，然后存放在dirPath的一个JSON文件里
// 保存的JSON文件名字是trailId_config.json，再dirPath中
// 文件则存在dirPath/file中
func (caecol *CAECollection) GetTrail(trailId, dirPath string) error {
	// 首先读取所有的traiId对应的参数
	j, err := caecol.GetData(trailId, "")
	if err != nil {
		return err
	}
	delete(j, "_id") // 删除id
	for paramName, paramValue := range j {
		fmt.Println(paramName, paramValue)
		switch pv := paramValue.(type) {
		case int:
		case float32:
		case float64:
		case string:
			if isFile(pv) {
				// 只有是file时才需要保存这个文件
				err := caecol.GetFile(trailId, paramName, filepath.Join(dirPath, pv))
				if err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("有未识别的类型:%v", paramValue)
		}
	}

	jsonBytes, err := json.MarshalIndent(j, "", "\t")
	if err != nil {
		return err
	}

	savePath := trailId + "_config.json"
	f, err := os.OpenFile(
		filepath.Join(dirPath, savePath), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	// 如果文件已经存在，则先删除已经存在的
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(jsonBytes) // 写入文件
	if err != nil {
		return err
	}

	fmt.Println("下载的文件保存在了：", filepath.Join(dirPath, "file"))
	fmt.Println("jsonfile保存在了：", savePath)

	return nil
}

func (caecol *CAECollection) deleteData(trailId string, paramName string) error {

	if _, err := caecol.CollectionObj.UpdateOne(
		context.Background(),
		bson.M{"_id": trailId},
		bson.M{"$unset": bson.M{paramName: ""}}); err != nil {
		// 利用unset删除
		return fmt.Errorf("删除数据trailId: %v, paramName: %v时出错：%v", trailId, paramName, err)
	}

	return nil
}
func (caecol *CAECollection) DeleteFile(trailId string, fileName string) error {

	res, err := caecol.GetData(trailId, fileName)
	if err != nil {
		return err
	}
	fileNameInMinIO, ok := res[fileName].(string)
	if !ok {
		return fmt.Errorf("程序判断这个内容是文件，请检查是否是文件：%v", fileName)
	}

	// 不能以./  ../ 开头
	fileNameInMinIO = strings.TrimLeft(fileNameInMinIO, "./")
	fileNameInMinIO = strings.TrimLeft(fileNameInMinIO, "../")

	// 先删除文件
	err = caecol.minioC.RemoveObject(
		context.Background(),
		trailId,         // buckiet名字
		fileNameInMinIO, // MinIO中的名字
		minio.RemoveObjectOptions{},
	)
	if err != nil {
		return err
	}

	// 再删除mongodb中的字段
	return caecol.deleteData(trailId, fileName)
}

// 删除一个数据
func (caecol *CAECollection) DeleteData(trailId, paramName string) error {

	res, err := caecol.GetData(trailId, paramName)
	if err != nil {
		return err
	}

	if res[paramName] == nil {
		return nil
	}

	var deleteFunc func(string, string) error
	switch v := res[paramName].(type) {
	case int:
		deleteFunc = caecol.deleteData
	case int32:
		deleteFunc = caecol.deleteData
	case uint32:
		deleteFunc = caecol.deleteData
	case float32:
		deleteFunc = caecol.deleteData
	case float64:
		deleteFunc = caecol.deleteData
	case string:
		if isFile(v) {
			deleteFunc = caecol.DeleteFile
		} else {
			deleteFunc = caecol.deleteData
		}
	default:
		return fmt.Errorf("未识别的类型: %v", res[paramName])
	}

	return deleteFunc(trailId, paramName)
}

// 删除trailId对应的所有内容
func (caecol *CAECollection) DeleteTrail(trailId string) error {

	err := caecol.minioC.RemoveBucket(
		context.Background(),
		trailId,
	)
	if err != nil {
		return err
	}

	_, err = caecol.CollectionObj.DeleteOne(
		context.TODO(),
		bson.D{{"_id", trailId}},
		&options.DeleteOptions{},
	)
	if err != nil {
		return err
	}

	return nil
}

func MakeProjection(names ...string) Projection {

	if len(names) == 0 {
		return nil
	}
	var pro Projection
	for _, name := range names {
		pro = append(pro, bson.E{name, 1})
	}
	return pro

}

// 通过filter和pro查找
// 示例代码：
/*
filter := Filter{"_id": "0005"}
pro := MakeProjection("Speed", "deck", "TrailName")
fmt.Println(colobj.FindOne(filter, pro))
*/
func (caecol *CAECollection) FindOne(filter Filter, proj Projection) (map[string]interface{}, error) {

	var opts *options.FindOneOptions
	if proj != nil {
		opts = options.FindOne().SetProjection(proj)
	}

	var res map[string]interface{}
	err := caecol.CollectionObj.FindOne(
		context.TODO(),
		filter,
		opts,
	).Decode(&res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// 查找多个
func (caecol *CAECollection) Find(filter Filter, proj Projection) ([]map[string]interface{}, error) {

	var opts *options.FindOptions
	if proj != nil {
		opts = options.Find().SetProjection(proj)
	}

	cursor, err := caecol.CollectionObj.Find(
		context.TODO(),
		filter,
		opts,
	)
	if err != nil {
		return nil, err
	}

	var res []map[string]interface{}
	err = cursor.All(
		context.TODO(),
		&res,
	)
	if err != nil {
		return nil, err
	}

	return res, nil
}
func main() {

	var cae CAECollection
	endpoint := "172.17.0.154:9000"
	// endpoint := "150.158.17.239:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false // 没有安装证书的填false

	// Initialize minio client object. 建立连接
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// 连接到MongoDB
	client1, err := mongo.Connect(context.TODO(), clientOptions)
	collection := client1.Database("datebase").Collection("collection1")
	cae.minioC = minioClient
	cae.CollectionObj = collection

	cae.PushFile("001", "实验文件3", "F:/Visual_Studio_Code/data/dbm_fiber_last.zip")
	// str := "trailId=002&fileName=Test01&status=false"
	// fmt.Println("上传文件信息: " + str)
	// http.Get("http://localhost:8080/websocket?" + str)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

}
