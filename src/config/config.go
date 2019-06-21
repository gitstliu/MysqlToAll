package config

import (
	"io/ioutil"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gitstliu/log4go"
)

type CanalConfig struct {
	Cancalconfigpath string               `toml:"cancalconfigpath"`
	Posconfigfile    string               `toml:"posconfigfile"`
	Bulksize         int                  `toml:"bulksize"`
	Flushbulktime    int64                `toml:"flushbulktime"`
	CacheSize        int64                `toml:"cachesize"`
	Redis            *RedisConfig         `toml:"redis"`
	RedisCluster     *RedisClusterConfig  `toml:"redis-cluster"`
	Elasticsearch    *ElasticsearchConfig `toml:"elasticsearch"`
	Kafka            *KafkaConfig         `toml:"kafka"`
	Datafile         *DatafileConfig      `toml:"datafile"`
	LogPos           *Pos
	//	Target           map[string]string    `toml:"target"`
}

type Pos struct {
	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
}

type CommonConfig interface {
	GetConfigName() string
}

type RedisConfig struct {
	CommonConfig
	Address  string                       `toml:"address"`
	Password string                       `toml:"password"`
	DB       int                          `toml:"db"`
	Tables   map[string]*RedisTableConfig `toml:"tables"`
}

func (this *RedisConfig) GetConfigName() string {
	return "Redis"
}

type RedisTableConfig struct {
	Tablename  string   `toml:"tablename"`
	Actions    []string `toml:"actions"`
	Struct     string   `toml:"struct"`
	Key        []string `toml:"key"`
	Keysplit   string   `toml:"keysplit"`
	Valuetype  string   `toml:"valuetype"`
	Valuesplit string   `toml:"valuesplit"`
	KeyPrefix  string   `toml:"keyprefix"`
	KeyPostfix string   `toml:"keypostfix"`
	Reidskey   string   `toml:"reidskey"`
}

type RedisClusterConfig struct {
	CommonConfig
	Address      []string                     `toml:"address"`
	ReadTimeout  int64                        `toml:"readtimeout"`
	ConnTimeout  int64                        `toml:"conntimeout"`
	WriteTimeout int64                        `toml:"writetimeout"`
	AliveTime    int64                        `toml:"alivetime"`
	Keepalive    int                          `toml:"keepalive"`
	Tables       map[string]*RedisTableConfig `toml:"tables"`
}

func (this *RedisClusterConfig) GetConfigName() string {
	return "RedisCluster"
}

type ElasticsearchConfig struct {
	CommonConfig
	Address  string                               `toml:"address"`
	User     string                               `toml:"user"`
	Password string                               `toml:"password"`
	IsHttps  bool                                 `toml:"ishttps"`
	Tables   map[string]*ElasticsearchTableConfig `toml:"tables"`
}

type ElasticsearchTableConfig struct {
	Tablename  string   `toml:"tablename"`
	Actions    []string `toml:"actions"`
	Index      string   `toml:"index"`
	IndexType  string   `toml:"indextype"`
	Key        []string `toml:"key"`
	Keysplit   string   `toml:"keysplit"`
	KeyPrefix  string   `toml:"keyprefix"`
	KeyPostfix string   `toml:"keypostfix"`
}

func (this *ElasticsearchConfig) GetConfigName() string {
	return "Elasticsearch"
}

type KafkaConfig struct {
	CommonConfig
	Address string                       `toml:"address"`
	Tables  map[string]*KafkaTableConfig `toml:"tables"`
}

type KafkaTableConfig struct {
	Tablename string   `toml:"tablename"`
	Actions   []string `toml:"actions"`
	Topic     []string `toml:"topic"`
}

func (this *KafkaConfig) GetConfigName() string {
	return "Kafka"
}

type DatafileConfig struct {
	CommonConfig
	Filename string `toml:"filename"`
}

func (this *DatafileConfig) GetConfigName() string {
	return "Datafile"
}

type Configure struct {
	//	Datafile     DatafileConfig          `toml:"datafile"`
	CanalConfigs map[string]*CanalConfig `toml:"canal"`
}

var configure *Configure

func LoadConfigWithFile(name string) error {
	data, readFileErr := ioutil.ReadFile(name)
	if readFileErr != nil {
		log4go.Error(readFileErr)
		return readFileErr
	}

	conf := &Configure{}
	_, decodeTomlErr := toml.Decode(string(data), &conf)
	if decodeTomlErr != nil {
		log4go.Error(decodeTomlErr)
		return decodeTomlErr
	}

	for _, currCanalConfig := range conf.CanalConfigs {
		if strings.Trim(currCanalConfig.Posconfigfile, " ") != "" {
			currPos, readPosErr := ioutil.ReadFile(currCanalConfig.Posconfigfile)
			if readPosErr != nil {
				log4go.Error(readPosErr)
				return readPosErr
			}
			pos := &Pos{}
			_, decodePosErr := toml.Decode(string(currPos), &pos)

			if decodePosErr != nil {
				log4go.Error(decodePosErr)
				return decodePosErr
			}

			if pos.Name != "" {
				currCanalConfig.LogPos = pos
			}
		}

	}

	configure = conf

	return nil
}

func GetConfigure() *Configure {
	return configure
}
