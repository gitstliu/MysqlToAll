package adapter

import (
	"adapter/common"
	"adapter/datafile"
	"adapter/elasticsearch"
	"adapter/kafka"
	"adapter/redis"
	"adapter/rediscluster"
	"config"
	"errors"

	"github.com/gitstliu/log4go"
)

func CreateAdapterWithName(conf config.CommonConfig) (common.WriteAdapter, error) {
	if conf.GetConfigName() == "Redis" {
		return redisadapter.CreateAdapter(conf.(*config.RedisConfig)), nil
	} else if conf.GetConfigName() == "RedisCluster" {
		return redisclusteradapter.CreateAdapter(conf.(*config.RedisClusterConfig)), nil
	} else if conf.GetConfigName() == "Elasticsearch" {
		return elasticsearchadapter.CreateAdapter(conf.(*config.ElasticsearchConfig)), nil
	} else if conf.GetConfigName() == "Kafka" {
		return kafkaadapter.CreateAdapter(conf.(*config.KafkaConfig)), nil
	} else if conf.GetConfigName() == "Datafile" {
		return datafileadapter.CreateAdapter(conf.(*config.DatafileConfig)), nil
	}

	log4go.Error("Config Type %v is not support !!!!", conf.GetConfigName())
	return nil, errors.New("Config Type Error!")
}
