package kafkaadapter

import (
	"adapter/common"
	"client/kafkaclient"
	"config"

	"github.com/gitstliu/log4go"
)

type KafkaAdapter struct {
	common.WriteAdapter
	kafkaClient    *kafkaclient.Client
	Config         *config.KafkaConfig
	TableActionMap map[string]map[string][]*config.KafkaTableConfig
}

func CreateAdapter(conf *config.KafkaConfig) common.WriteAdapter {

	adapter := &KafkaAdapter{Config: conf}
	adapter.TableActionMap = DecoderAdapterTableMessage(conf.Tables)
	clientConfig := &kafkaclient.ClientConfig{}
	clientConfig.Address = conf.Address

	adapter.kafkaClient = kafkaclient.NewClient(clientConfig)

	if adapter.kafkaClient == nil {
		log4go.Error("Can not create kafka client !!")
		return nil
	}

	return adapter
}

func (this *KafkaAdapter) Write(entities []*common.RawLogEntity) error {

	//	jsons := []string{}
	for _, currEntity := range entities {
		entityJson, entityJsonErr := currEntity.ToJson()
		if entityJsonErr != nil {
			log4go.Error(entityJsonErr)
			return entityJsonErr
		}

		currConfigs := this.GetTableActionConfigs(currEntity.TableName, currEntity.Action)

		for _, currConfig := range currConfigs {
			sendMessagesErr := this.kafkaClient.SendMessages([]string{entityJson}, currConfig.Topic)
			if sendMessagesErr != nil {
				log4go.Error(sendMessagesErr)
				return sendMessagesErr
			}
		}
	}

	return nil
}

func (this *KafkaAdapter) Close() error {
	this.kafkaClient.Close()
	return nil
}

func DecoderAdapterTableMessage(configs map[string]*config.KafkaTableConfig) map[string]map[string][]*config.KafkaTableConfig {
	tableActionMap := map[string]map[string][]*config.KafkaTableConfig{}
	for _, tableConfig := range configs {
		if len(tableConfig.Actions) > 0 {
			_, currActionMapExist := tableActionMap[tableConfig.Tablename]
			if !currActionMapExist {
				tableActionMap[tableConfig.Tablename] = map[string][]*config.KafkaTableConfig{}
			}
			for _, currAction := range tableConfig.Actions {
				_, actionConfigListExist := tableActionMap[tableConfig.Tablename][currAction]
				if !actionConfigListExist {
					tableActionMap[tableConfig.Tablename][currAction] = []*config.KafkaTableConfig{}
				}
				tableActionMap[tableConfig.Tablename][currAction] = append(tableActionMap[tableConfig.Tablename][currAction], tableConfig)
			}
		}
	}
	return tableActionMap
}

func (this *KafkaAdapter) GetTableActionConfigs(table, action string) []*config.KafkaTableConfig {
	_, tableExist := this.TableActionMap[table]
	if !tableExist {
		return nil
	}
	actionConfigs, actionExist := this.TableActionMap[table][action]
	if !actionExist {
		return nil
	}

	return actionConfigs
}
