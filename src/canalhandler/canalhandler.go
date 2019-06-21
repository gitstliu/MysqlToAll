package canalhandler

import (
	"binloghandler"
	"config"
	"output"

	"github.com/gitstliu/log4go"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
)

type CommonCanalMeta struct {
	Name           string
	ConfigFilePath string
	Config         *canal.Config
	Canal          *canal.Canal
	CurrOutput     *output.Output
	BinData        chan interface{}
}

//func (this *CommonCanalMeta) RunWithConfig(filePath string, name string, out *output.Output, pos *config.Pos) {
func (this *CommonCanalMeta) RunWithConfig(name string, conf *config.CanalConfig) {

	//	log4go.Info("Start bin log at %v %v", conf.LogPos.Name, conf.LogPos.Pos)
	currOutput, createOutputErr := output.CreateByName(name)

	if createOutputErr != nil {
		log4go.Error(createOutputErr)
		panic(createOutputErr)
		return
	}

	this.BinData = make(chan interface{}, 4096)

	this.Name = name
	this.CurrOutput = currOutput
	cfg, loadConfigErr := canal.NewConfigWithFile(conf.Cancalconfigpath)
	if loadConfigErr != nil {
		log4go.Error(loadConfigErr)
		panic(loadConfigErr)
	}
	currCanal, createCanalErr := canal.NewCanal(cfg)

	if createCanalErr != nil {
		log4go.Error(createCanalErr)
		panic(createCanalErr)
	}

	go this.CurrOutput.Run()
	currCanal.SetEventHandler(&handler.CommonEventHandler{CurrOutput: this.CurrOutput})
	if conf.LogPos != nil {
		startPos := mysql.Position{Name: conf.LogPos.Name, Pos: conf.LogPos.Pos}
		log4go.Info("Run with pos")
		currCanal.RunFrom(startPos)
	} else {
		log4go.Info("Run without pos")
		currCanal.Run()
	}

}

//func (this *CommonCanalMeta) RunSync() {
//	for true {
//		currData := <-this.BinData
//	}
//}
