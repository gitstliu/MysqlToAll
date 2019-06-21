package redisadapter

import (
	"adapter/common"
	"config"
	"errors"
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/gitstliu/go-commonfunctions"
	"github.com/gitstliu/log4go"
)

type RedisPipelineCommand struct {
	CommandName string
	Key         string
	Args        []interface{}
}

type RedisAdapter struct {
	common.WriteAdapter
	redisClient    redis.Conn
	Config         *config.RedisConfig
	TableActionMap map[string]map[string][]*config.RedisTableConfig
	TableKeyMap    map[string]map[string][]*config.RedisTableConfig
}

func CreateAdapter(conf *config.RedisConfig) common.WriteAdapter {
	adapter := &RedisAdapter{Config: conf}
	adapter.TableActionMap, adapter.TableKeyMap = DecoderAdapterTableMessage(conf.Tables)
	if conf.Password != "" {
		option := redis.DialPassword(conf.Password)
		currClient, redisConnErr := redis.Dial("tcp", conf.Address, option)
		if redisConnErr != nil {
			log4go.Error(redisConnErr)
			panic(redisConnErr)
		}
		adapter.redisClient = currClient
	} else {
		currClient, redisConnErr := redis.Dial("tcp", conf.Address)
		if redisConnErr != nil {
			log4go.Error(redisConnErr)
			panic(redisConnErr)
		}
		adapter.redisClient = currClient
	}

	adapter.redisClient.Do("SELECT", conf.DB)
	return adapter
}

func DecoderAdapterTableMessage(configs map[string]*config.RedisTableConfig) (map[string]map[string][]*config.RedisTableConfig, map[string]map[string][]*config.RedisTableConfig) {
	tableActionMap := map[string]map[string][]*config.RedisTableConfig{}
	tableKeyMap := map[string]map[string][]*config.RedisTableConfig{}
	for _, tableConfig := range configs {
		if len(tableConfig.Actions) > 0 {
			_, currActionMapExist := tableActionMap[tableConfig.Tablename]
			if !currActionMapExist {
				tableActionMap[tableConfig.Tablename] = map[string][]*config.RedisTableConfig{}
			}
			for _, currAction := range tableConfig.Actions {
				_, actionConfigListExist := tableActionMap[tableConfig.Tablename][currAction]
				if !actionConfigListExist {
					tableActionMap[tableConfig.Tablename][currAction] = []*config.RedisTableConfig{}
				}
				tableActionMap[tableConfig.Tablename][currAction] = append(tableActionMap[tableConfig.Tablename][currAction], tableConfig)
			}
		}

		if len(tableConfig.Key) > 0 {
			_, currKeyMapExist := tableKeyMap[tableConfig.Tablename]
			if !currKeyMapExist {
				tableKeyMap[tableConfig.Tablename] = map[string][]*config.RedisTableConfig{}
			}
			for _, currKey := range tableConfig.Key {
				_, actionConfigListExist := tableKeyMap[tableConfig.Tablename][currKey]
				if !actionConfigListExist {
					tableKeyMap[tableConfig.Tablename][currKey] = []*config.RedisTableConfig{}
				}
				tableKeyMap[tableConfig.Tablename][currKey] = append(tableKeyMap[tableConfig.Tablename][currKey], tableConfig)
			}
		}
	}
	return tableActionMap, tableKeyMap
}

func (this *RedisAdapter) GetTableActionConfigs(table, action string) []*config.RedisTableConfig {
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

func (this *RedisAdapter) GetTableKeyConfigs(table, key string) []*config.RedisTableConfig {
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

func (this *RedisAdapter) Write(entities []*common.RawLogEntity) error {
	commands := []*RedisPipelineCommand{}

	for _, currEntity := range entities {
		tableActionsConfig, tableConfigExist := this.TableActionMap[currEntity.TableName]
		if !tableConfigExist {
			continue
		}

		for currAction, currConfigs := range tableActionsConfig {
			if currAction == currEntity.Action {
				for _, currConfig := range currConfigs {
					var currCommand *RedisPipelineCommand
					var creatCommandErr error
					if currConfig.Struct == "string" {
						if currEntity.Action == "update" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForStringSet(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "insert" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForStringSet(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "delete" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForStringDel(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						}

					} else if currConfig.Struct == "list" {
						if currEntity.Action == "update" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForListRPush(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "insert" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForListRPush(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "delete" {
							continue
						}
					} else if currConfig.Struct == "set" {
						if currEntity.Action == "update" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForSet(currConfig, currEntity, "SADD")
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "insert" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForSet(currConfig, currEntity, "SADD")
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "delete" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForSet(currConfig, currEntity, "SREM")
							log4go.Debug("currCommand = %v", currCommand)
						}
					} else if currConfig.Struct == "hash" {
						if currEntity.Action == "update" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForHashHSet(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "insert" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForHashHSet(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						} else if currEntity.Action == "delete" {
							currCommand, creatCommandErr = CreateRedisPipelineCommandForHashHDel(currConfig, currEntity)
							log4go.Debug("currCommand = %v", currCommand)
						}
					} else {
						log4go.Error("Redis data struct exception. struct is %v", currConfig.Struct)
						return errors.New(fmt.Sprintf("Redis-Cluster data struct exception. struct is %v", currConfig.Struct))
					}
					if creatCommandErr != nil {
						log4go.Error(creatCommandErr)
						return creatCommandErr
					}

					if currCommand != nil {
						log4go.Debug(commonfunctions.ObjectToJson(currCommand))
						commands = append(commands, currCommand)
					}
				}
			}
		}
	}
	_, commandsSendErrors := this.SendPipelineCommands(commands)
	for _, currErr := range commandsSendErrors {
		if currErr != nil {
			return currErr
		}
	}

	return nil
}

func CreateRedisPipelineCommandForStringSet(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity) (*RedisPipelineCommand, error) {

	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = "SET"
	keyMeta := []interface{}{}
	for _, currKey := range currConfig.Key {
		keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
	}
	keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

	bodyValue := ""
	if currConfig.Valuetype == "json" {
		valueMeta := map[string]interface{}{}
		for columnIndex, columnName := range currEntity.Header {
			valueMeta[columnName] = currEntity.Rows[len(currEntity.Rows)-1][columnIndex]
		}
		valueMetaJson, valueMetaJsonErr := commonfunctions.ObjectToJson(valueMeta)
		if valueMetaJsonErr != nil {
			log4go.Error(valueMetaJsonErr)
			panic(valueMetaJsonErr)
			return nil, valueMetaJsonErr
		}
		bodyValue = valueMetaJson
	} else if currConfig.Valuetype == "splitstring" {
		valueMeta := currEntity.Rows[len(currEntity.Rows)-1]
		bodyValue = strings.Join(commonfunctions.InterfacesToStringsConverter(valueMeta), currConfig.Valuesplit)
	} else {
		log4go.Error("Error valuetype %v", currConfig.Valuetype)
		valueTypeErr := errors.New("Error valuetype")
		panic(valueTypeErr)
		return nil, valueTypeErr
	}

	currCommand.Key = keyValue
	currCommand.Args = []interface{}{bodyValue}
	return currCommand, nil
}

func CreateRedisPipelineCommandForStringDel(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity) (*RedisPipelineCommand, error) {

	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = "DEL"
	keyMeta := []interface{}{}
	for _, currKey := range currConfig.Key {
		keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
	}
	keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

	currCommand.Key = keyValue
	currCommand.Args = []interface{}{}
	return currCommand, nil
}

func CreateRedisPipelineCommandForListRPush(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity) (*RedisPipelineCommand, error) {

	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = "RPUSH"
	keyValue := currConfig.Reidskey

	bodyValue := ""
	if currConfig.Valuetype == "json" {
		valueMeta := map[string]interface{}{}
		for columnIndex, columnName := range currEntity.Header {
			valueMeta[columnName] = currEntity.Rows[len(currEntity.Rows)-1][columnIndex]
		}
		valueMetaJson, valueMetaJsonErr := commonfunctions.ObjectToJson(valueMeta)
		if valueMetaJsonErr != nil {
			log4go.Error(valueMetaJsonErr)
			panic(valueMetaJsonErr)
			return nil, valueMetaJsonErr
		}
		bodyValue = valueMetaJson
	} else if currConfig.Valuetype == "splitstring" {
		valueMeta := currEntity.Rows[len(currEntity.Rows)-1]
		bodyValue = strings.Join(commonfunctions.InterfacesToStringsConverter(valueMeta), currConfig.Valuesplit)
	} else {
		log4go.Error("Error valuetype %v", currConfig.Valuetype)
		valueTypeErr := errors.New("Error valuetype")
		panic(valueTypeErr)
		return nil, valueTypeErr
	}

	currCommand.Key = keyValue
	currCommand.Args = []interface{}{bodyValue}
	return currCommand, nil
}

func CreateRedisPipelineCommandForSet(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity, command string) (*RedisPipelineCommand, error) {
	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = command
	keyValue := currConfig.Reidskey

	bodyValue := ""
	if currConfig.Valuetype == "json" {
		valueMeta := map[string]interface{}{}
		for columnIndex, columnName := range currEntity.Header {
			valueMeta[columnName] = currEntity.Rows[len(currEntity.Rows)-1][columnIndex]
		}
		valueMetaJson, valueMetaJsonErr := commonfunctions.ObjectToJson(valueMeta)
		if valueMetaJsonErr != nil {
			log4go.Error(valueMetaJsonErr)
			panic(valueMetaJsonErr)
			return nil, valueMetaJsonErr
		}
		bodyValue = valueMetaJson
	} else if currConfig.Valuetype == "splitstring" {
		valueMeta := currEntity.Rows[len(currEntity.Rows)-1]
		bodyValue = strings.Join(commonfunctions.InterfacesToStringsConverter(valueMeta), currConfig.Valuesplit)
	} else {
		log4go.Error("Error valuetype %v", currConfig.Valuetype)
		valueTypeErr := errors.New("Error valuetype")
		panic(valueTypeErr)
		return nil, valueTypeErr
	}

	currCommand.Key = keyValue
	currCommand.Args = []interface{}{bodyValue}
	return currCommand, nil
}

func CreateRedisPipelineCommandForHashHSet(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity) (*RedisPipelineCommand, error) {
	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = "HSET"

	keyMeta := []interface{}{}
	for _, currKey := range currConfig.Key {
		keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
	}
	keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

	bodyValue := ""
	if currConfig.Valuetype == "json" {
		valueMeta := map[string]interface{}{}
		for columnIndex, columnName := range currEntity.Header {
			valueMeta[columnName] = currEntity.Rows[len(currEntity.Rows)-1][columnIndex]
		}
		valueMetaJson, valueMetaJsonErr := commonfunctions.ObjectToJson(valueMeta)
		if valueMetaJsonErr != nil {
			log4go.Error(valueMetaJsonErr)
			panic(valueMetaJsonErr)
			return nil, valueMetaJsonErr
		}
		bodyValue = valueMetaJson
	} else if currConfig.Valuetype == "splitstring" {
		valueMeta := currEntity.Rows[len(currEntity.Rows)-1]
		bodyValue = strings.Join(commonfunctions.InterfacesToStringsConverter(valueMeta), currConfig.Valuesplit)
	} else {
		log4go.Error("Error valuetype %v", currConfig.Valuetype)
		valueTypeErr := errors.New("Error valuetype")
		panic(valueTypeErr)
		return nil, valueTypeErr
	}

	currCommand.Key = currConfig.Reidskey
	currCommand.Args = []interface{}{keyValue, bodyValue}
	return currCommand, nil
}

func CreateRedisPipelineCommandForHashHDel(currConfig *config.RedisTableConfig, currEntity *common.RawLogEntity) (*RedisPipelineCommand, error) {
	currCommand := &RedisPipelineCommand{}
	currCommand.CommandName = "HDEL"

	keyMeta := []interface{}{}
	for _, currKey := range currConfig.Key {
		keyMeta = append(keyMeta, currEntity.Rows[len(currEntity.Rows)-1][currEntity.HeaderMap[currKey]])
	}
	keyValue := strings.Join([]string{currConfig.KeyPrefix, strings.Join(commonfunctions.InterfacesToStringsConverter(keyMeta), currConfig.Keysplit), currConfig.KeyPostfix}, ":")

	currCommand.Key = currConfig.Reidskey
	currCommand.Args = []interface{}{keyValue}
	return currCommand, nil
}

func (this *RedisAdapter) Close() error {
	closeErr := this.redisClient.Close()
	if closeErr != nil {
		log4go.Error(closeErr)
	}
	return closeErr
}

func (this *RedisAdapter) SET(key, value string) (string, error) {
	log4go.Debug("key is %v", key)
	return redis.String(this.redisClient.Do("SET", key, value))
}

func (this *RedisAdapter) GET(key string) (string, error) {
	log4go.Debug("key is %v", key)
	return redis.String(this.redisClient.Do("GET", key))
}

func (this *RedisAdapter) KEYS(key string) ([]string, error) {
	log4go.Debug("key is %v", key)
	return redis.Strings(this.redisClient.Do("KEYS", key))
}

func (this *RedisAdapter) LPUSH(key string, value []interface{}) (interface{}, error) {
	log4go.Debug("key is %v, value is %v", key, value)
	return this.redisClient.Do("LPUSH", append([](interface{}){key}, value...)...)
}

func (this *RedisAdapter) RPUSH(key string, value []interface{}) (interface{}, error) {
	log4go.Debug("key is %v, value is %v", key, value)
	return this.redisClient.Do("RPUSH", append([](interface{}){key}, value...)...)
}

func (this *RedisAdapter) LPOP(key string) (string, error) {
	return redis.String(this.redisClient.Do("LPOP", key))
}

func (this *RedisAdapter) LRANGE(key string, index int, endIndex int) ([]string, error) {
	return redis.Strings(this.redisClient.Do("LRANGE", key, index, endIndex))
}

func (this *RedisAdapter) SendPipelineCommands(commands []*RedisPipelineCommand) ([]interface{}, []error) {
	//	defer commonfunctions.PanicHandler()
	log4go.Debug("commands %v", commands)
	errorList := make([]error, 0, len(commands)+1)

	for index, value := range commands {
		log4go.Debug("Curr Commands index is %v value is %v", index, value)
		log4go.Debug("********************")
		log4go.Debug("%v", [](interface{}){value.Key})

		for in, v := range value.Args {
			log4go.Debug("===== %v %v", in, v)
		}

		log4go.Debug("%v", value.Args...)
		log4go.Debug("%v", append([](interface{}){value.Key}, value.Args...))
		log4go.Debug("%v", append([](interface{}){value.Key}, value.Args...)...)
		log4go.Debug("CommandName is %v", value.CommandName)
		currErr := this.redisClient.Send(value.CommandName, append([](interface{}){value.Key}, value.Args...)...)

		if currErr != nil {
			log4go.Error(currErr)
			errorList = append(errorList, currErr)
			log4go.Debug("command === %v", value)
		}
	}

	log4go.Debug("Send finished!!")

	fulshErr := this.redisClient.Flush()

	if fulshErr != nil {
		log4go.Error(fulshErr)
		errorList = append(errorList, fulshErr)

		return nil, errorList
	}

	replys := [](interface{}){}

	replysLength := len(commands)

	for i := 0; i < replysLength; i++ {
		reply, receiveErr := this.redisClient.Receive()

		if receiveErr != nil {
			log4go.Error(receiveErr)
			errorList = append(errorList, receiveErr)
		}

		replys = append(replys, reply)
	}

	log4go.Debug("Receive finished!!")

	if len(errorList) != 0 {
		return replys, errorList
	}

	return replys, nil
}
