package elasticsearchadapter

import (
	"adapter/common"
	"client/esclient"
	"config"
	"errors"
	"strings"

	"github.com/gitstliu/go-commonfunctions"
	"github.com/gitstliu/log4go"
)

type ElasticsearchAdapter struct {
	common.WriteAdapter
	esClient       *esclient.Client
	Config         *config.ElasticsearchConfig
	TableActionMap map[string]map[string][]*config.ElasticsearchTableConfig
	TableKeyMap    map[string]map[string][]*config.ElasticsearchTableConfig
}

func CreateAdapter(conf *config.ElasticsearchConfig) common.WriteAdapter {

	adapter := &ElasticsearchAdapter{Config: conf}
	adapter.TableActionMap, adapter.TableKeyMap = DecoderAdapterTableMessage(conf.Tables)
	clientConfig := &esclient.ClientConfig{}
	clientConfig.Addr = conf.Address
	clientConfig.HTTPS = conf.IsHttps
	clientConfig.User = conf.User
	clientConfig.Password = conf.Password
	adapter.esClient = esclient.NewClient(clientConfig)

	if adapter.esClient == nil {
		log4go.Error("Can not create es client !!")
		return nil
	}

	return adapter
}

func (this *ElasticsearchAdapter) Write(entities []*common.RawLogEntity) error {

	esRequest := []*esclient.BulkRequest{}

	for _, currEntity := range entities {

		currConfigs := this.GetTableActionConfigs(currEntity.TableName, currEntity.Action)

		log4go.Debug("currEntity.Action = %v", currEntity.Action)
		if currEntity.Action == "insert" || currEntity.Action == "update" {
			for _, currConfig := range currConfigs {
				keyMeta := []interface{}{}
				for _, currKey := range currConfig.Key {
					keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
				}
				keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

				currRequest := &esclient.BulkRequest{}
				currRequest.Action = "index"
				currRequest.Data = currEntity.ValueMap
				currRequest.Index = currConfig.Index
				currRequest.ID = keyValue
				currRequest.Type = currConfig.IndexType

				esRequest = append(esRequest, currRequest)
			}
		} else if currEntity.Action == "delete" {
			for _, currConfig := range currConfigs {
				keyMeta := []interface{}{}
				for _, currKey := range currConfig.Key {
					keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
				}
				keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

				currRequest := &esclient.BulkRequest{}
				currRequest.Action = "delete"
				currRequest.Index = currConfig.Index
				currRequest.ID = keyValue
				currRequest.Type = currConfig.IndexType

				esRequest = append(esRequest, currRequest)
			}
		}
	}

	log4go.Debug("esRequest = %v", esRequest)
	bulkErr := this.doBulk(this.Config.Address, esRequest)
	return bulkErr
}

func (this *ElasticsearchAdapter) Close() error {
	return nil
}

func (this *ElasticsearchAdapter) doBulk(url string, reqs []*esclient.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	resp, err := this.esClient.DoBulk(url, reqs)
	if err != nil {
		log4go.Error("sync docs err %v", err)
		return err
	} else if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log4go.Error("%v index: %v, type: %v, id: %v, status: %v, error: %v",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
					return errors.New(string(item.Error))
				}
			}
		}
	}

	return nil
}

func DecoderAdapterTableMessage(configs map[string]*config.ElasticsearchTableConfig) (map[string]map[string][]*config.ElasticsearchTableConfig, map[string]map[string][]*config.ElasticsearchTableConfig) {
	tableActionMap := map[string]map[string][]*config.ElasticsearchTableConfig{}
	tableKeyMap := map[string]map[string][]*config.ElasticsearchTableConfig{}
	for _, tableConfig := range configs {
		if len(tableConfig.Actions) > 0 {
			_, currActionMapExist := tableActionMap[tableConfig.Tablename]
			if !currActionMapExist {
				tableActionMap[tableConfig.Tablename] = map[string][]*config.ElasticsearchTableConfig{}
			}
			for _, currAction := range tableConfig.Actions {
				_, actionConfigListExist := tableActionMap[tableConfig.Tablename][currAction]
				if !actionConfigListExist {
					tableActionMap[tableConfig.Tablename][currAction] = []*config.ElasticsearchTableConfig{}
				}
				tableActionMap[tableConfig.Tablename][currAction] = append(tableActionMap[tableConfig.Tablename][currAction], tableConfig)
			}
		}

		if len(tableConfig.Key) > 0 {
			_, currKeyMapExist := tableKeyMap[tableConfig.Tablename]
			if !currKeyMapExist {
				tableKeyMap[tableConfig.Tablename] = map[string][]*config.ElasticsearchTableConfig{}
			}
			for _, currKey := range tableConfig.Key {
				_, actionConfigListExist := tableKeyMap[tableConfig.Tablename][currKey]
				if !actionConfigListExist {
					tableKeyMap[tableConfig.Tablename][currKey] = []*config.ElasticsearchTableConfig{}
				}
				tableKeyMap[tableConfig.Tablename][currKey] = append(tableKeyMap[tableConfig.Tablename][currKey], tableConfig)
			}
		}
	}
	return tableActionMap, tableKeyMap
}

func (this *ElasticsearchAdapter) GetTableActionConfigs(table, action string) []*config.ElasticsearchTableConfig {
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

func (this *ElasticsearchAdapter) GetTableKeyConfigs(table, key string) []*config.ElasticsearchTableConfig {
	_, tableExist := this.TableKeyMap[table]
	if !tableExist {
		return nil
	}
	keyConfigs, keyExist := this.TableKeyMap[table][key]
	if !keyExist {
		return nil
	}

	return keyConfigs
}
