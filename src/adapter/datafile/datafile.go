package datafileadapter

import (
	"adapter/common"
	"config"
	"os"
	"path"

	"github.com/gitstliu/log4go"
)

type DatafileAdapter struct {
	common.WriteAdapter
	Config   *config.DatafileConfig
	file     *os.File
	Filepath string
	Filename string
}

func CreateAdapter(conf *config.DatafileConfig) common.WriteAdapter {
	adapter := &DatafileAdapter{Config: conf}
	adapter.Filepath, adapter.Filename = path.Split(adapter.Config.Filename)

	mkdirErr := os.MkdirAll(adapter.Filepath, os.ModePerm)
	if mkdirErr != nil {
		log4go.Error(mkdirErr)
		panic(mkdirErr)
	}

	currFile, openFileErr := os.OpenFile(adapter.Config.Filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if openFileErr != nil {
		log4go.Error(openFileErr)
		panic(openFileErr)
	}
	adapter.file = currFile

	return adapter
}

func (this *DatafileAdapter) Write(entities []*common.RawLogEntity) error {

	content := ""
	for _, currEntity := range entities {
		content += currEntity.ToString()
	}
	_, writeToFileErr := this.file.WriteString(content)
	if writeToFileErr != nil {
		log4go.Error(writeToFileErr)
		panic(writeToFileErr)
		return writeToFileErr
	}
	syncToDiskErr := this.file.Sync()

	if syncToDiskErr != nil {
		log4go.Error(syncToDiskErr)
		panic(syncToDiskErr)
		return syncToDiskErr
	}

	return nil
}

func (this *DatafileAdapter) Close() error {
	closeFileErr := this.file.Close()
	if closeFileErr != nil {
		log4go.Error(closeFileErr)
		return closeFileErr
	}
	return nil
}
