package output

import (
	"adapter"
	"adapter/common"
	"config"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"sync"

	"github.com/siddontang/go/ioutil2"

	"github.com/gitstliu/log4go"
	"github.com/siddontang/go-mysql/mysql"
)

type Output struct {
	Config *config.CanalConfig
	//	PosFile         *os.File
	Adapters        map[string]common.WriteAdapter
	DataChannel     chan interface{}
	Datas           []interface{}
	lastWriteTime   time.Time
	writeLock       *sync.Mutex
	writeDataLength int64
}

func CreateByName(name string) (*Output, error) {

	currConfig, isConfigExist := config.GetConfigure().CanalConfigs[name]
	if !isConfigExist {
		return nil, errors.New(fmt.Sprintf("Output Config is not exist for name %s!!", name))
	}
	currOutput := &Output{}
	currOutput.Config = currConfig
	currOutput.Adapters = map[string]common.WriteAdapter{}
	//	currOutput.DataChannel = make(chan *common.RawLogEntity, currConfig.CacheSize)
	currOutput.DataChannel = make(chan interface{}, currConfig.CacheSize)
	currOutput.Datas = []interface{}{}
	currOutput.lastWriteTime = time.Now()
	currOutput.writeLock = &sync.Mutex{}
	posPath := path.Dir(currOutput.Config.Posconfigfile)
	makePosPathErr := os.MkdirAll(posPath, os.ModePerm)

	if makePosPathErr != nil {
		log4go.Error(makePosPathErr)
		panic(makePosPathErr)
	}

	//	currFile, openFileErr := os.OpenFile(currOutput.Config.Posconfigfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)

	//	if openFileErr != nil {
	//		log4go.Error(openFileErr)
	//		panic(openFileErr)
	//	}

	//	currOutput.PosFile = currFile

	if currConfig.Redis != nil {
		currOutput.Adapters[currConfig.Redis.GetConfigName()] = createAdapter(currConfig.Redis)
	}

	if currConfig.RedisCluster != nil {
		currOutput.Adapters[currConfig.RedisCluster.GetConfigName()] = createAdapter(currConfig.RedisCluster)
	}

	if currConfig.Elasticsearch != nil {
		currOutput.Adapters[currConfig.Elasticsearch.GetConfigName()] = createAdapter(currConfig.Elasticsearch)
	}

	if currConfig.Kafka != nil {
		currOutput.Adapters[currConfig.Kafka.GetConfigName()] = createAdapter(currConfig.Kafka)
	}

	if currConfig.Datafile != nil {
		currOutput.Adapters[currConfig.Datafile.GetConfigName()] = createAdapter(currConfig.Datafile)
	}

	return currOutput, nil
}

func createAdapter(conf config.CommonConfig) common.WriteAdapter {
	currAdapter, createAdapterErr := adapter.CreateAdapterWithName(conf)
	if createAdapterErr != nil {
		log4go.Error(createAdapterErr)
		panic(createAdapterErr)
		return nil
	}
	return currAdapter
}

func (this *Output) Run() {
	this.lastWriteTime = time.Now()
	go this.writeTimeProcess()
	for true {
		currData := <-this.DataChannel
		this.Datas = append(this.Datas, currData)
		dataLength := len(this.Datas)
		if dataLength >= this.Config.Bulksize {
			log4go.Info("Bulksize write")
			this.writeDataToAdapter()
		}
	}
}

func (this *Output) writeDataToAdapter() {
	log4go.Debug("Output write!!")
	this.writeLock.Lock()
	defer this.writeLock.Unlock()
	dataLength := len(this.Datas)
	if dataLength > 0 {
		mainData := []*common.RawLogEntity{}
		var posData *mysql.Position = nil
		for _, currData := range this.Datas {
			switch v := currData.(type) {
			case *common.RawLogEntity:
				mainData = append(mainData, v)
			case *mysql.Position:
				posData = v
			}
		}
		for adapterName, currAdapter := range this.Adapters {
			adapterWriteErr := currAdapter.Write(mainData)
			log4go.Debug("CanalConfig %v Adapter %v write data length %v", this.Config.Cancalconfigpath, adapterName, dataLength)
			//			log4go.Debug("Adapter is %v", currAdapter.(type))
			if adapterWriteErr != nil {
				log4go.Error(adapterWriteErr)
				panic(adapterWriteErr)
				configTimeDuration := this.Config.Flushbulktime * int64(time.Millisecond)
				time.Sleep(time.Duration(configTimeDuration))
				return
			}
		}

		if posData != nil {
			log4go.Info("Write Pos Data")
			binFileName := fmt.Sprintf("bin_name = \"%v\" \r\n", posData.Name)
			binFilePos := fmt.Sprintf("bin_pos = %v \r\n", posData.Pos)
			content := binFileName + binFilePos
			if err := ioutil2.WriteFileAtomic(this.Config.Posconfigfile, []byte(content), 0644); err != nil {
				log4go.Error("canal save master info to file %s err %v", this.Config.Posconfigfile, err)
			}
			//			truncateErr := this.PosFile.Truncate(0)
			//			if truncateErr != nil {
			//				log4go.Error(truncateErr)
			//				panic(truncateErr)
			//			}
			//			//			_, writePosErr := this.PosFile.WriteString(content)
			//			if writePosErr != nil {
			//				log4go.Error(writePosErr)
			//				panic(writePosErr)
			//			}
			//			//			syncPosErr := this.PosFile.Sync()
			//			if syncPosErr != nil {
			//				log4go.Error(syncPosErr)
			//				panic(syncPosErr)
			//			}
		}

		this.Datas = []interface{}{}
		this.lastWriteTime = time.Now()
		this.writeDataLength = this.writeDataLength + int64(dataLength)
		log4go.Info("Writed Data Length = %v", this.writeDataLength)
	}
}

func (this *Output) writeTimeProcess() {
	for true {
		currTimeDuration := time.Now().UnixNano() - this.lastWriteTime.UnixNano()
		configTimeDuration := this.Config.Flushbulktime * int64(time.Millisecond)
		if currTimeDuration >= configTimeDuration {
			log4go.Info("Time write")
			this.writeDataToAdapter()
			time.Sleep(time.Duration(configTimeDuration))
		} else {
			time.Sleep(time.Duration(configTimeDuration - currTimeDuration))
		}
	}
}

func (this *Output) Write(data interface{}) {
	this.DataChannel <- data
}
