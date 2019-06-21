package main

import (
	"canalhandler"
	"config"
	"time"

	"github.com/gitstliu/go-commonfunctions"
	"github.com/gitstliu/log4go"
)

func main() {

	log4go.LoadConfiguration("config/log.xml")

	defer log4go.Close()

	config.LoadConfigWithFile("config/config.toml")
	meta, _ := commonfunctions.ObjectToJson(config.GetConfigure())
	log4go.Debug(meta)

	log4go.Debug("********************************************")

	for name, currConfig := range config.GetConfigure().CanalConfigs {

		currCancal := &canalhandler.CommonCanalMeta{}
		//		go currCancal.RunWithConfig(currConfig.Cancalconfigpath, name, currOutput)
		go currCancal.RunWithConfig(name, currConfig)
		log4go.Info("Started %v", name)
	}

	for true {
		time.Sleep(10 * time.Second)
	}

}
