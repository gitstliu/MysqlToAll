# MysqlToAll

(部分功能未开源)
用途:
1. mysql到NoSQL,如ES、Redis、mongodb
2. mysql数据迁移,如postgresql
3. mysql到数据分析平台做数据聚合,如TiDB(待测试)
4. 数据审计

特点:
1. 支持多数据源多接收方
2. 单数据源配置多接收方时,支持近似数据同步(在出错的情况下可能相差最后一批数据)
3. redis及redis-cluster支持key格式化配置,支持json和分割字符串两种方式的数据存储。支持多种数据类型的处理
4. mongodb支持audit模式(可以把指定数据表的变化存入mongodb)和normal两种模式,并支持高速模式(打开时不支持upsert,关闭支持upsert)可以完全发挥mongodb的高性能
5. postgresql不支持upsert
6. mysql支持upsert(实现mysql的目的是为了支持TIDB)
7. 高性能,大部分中间件的数据同步在每秒5w条左右(性能会随着表列数增加而下降,随着同步中间的数量增多而下降)

注意:
1. 尽量选择支持upsert的用法
2. 尽量不要直接连接主库,而选择连接从库,以降低对主库的性能损耗
3. 一个数据源不要配置过多的接收方,这样会造成性能下降迅速(不要超过5个)

# 配置方法

## canalconfigs
```
xxx.toml canal配置文件
xxx.pos binlog解析位置文件
```

## config
```
config.toml 输出源配置文件
```
<img  src="https://github.com/gitstliu/MysqlToAll/blob/master/diss_config_ch.png"  alt="具体用法" align=center />

## QQ
<img  src="https://github.com/gitstliu/MysqlToAll/blob/master/QQ%E7%BE%A4%E5%90%8D%E7%89%87.png"  alt="QQ群" align=center />
加群的目的是? MysqlToAll


# 性能测试数据

测试机4C 8G


#### 场景：混合渠道-同时向redis、redis-cluster、kafka、es、本地文件串行写入。双数据库同步(两个mysql主同时同步binlog)
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 250%
内存：3%
开始时间：55:48
结束时间：58:46
总耗时：178秒
```


#### 场景：单一渠道-redis。单数据库同步
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 230%
内存：0.5%
开始时间：12:04
结束时间：12:31
总耗时：27秒
```

#### 场景：单一渠道-redis-cluster。单数据库同步
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 230%
内存：0.5%
开始时间：27:37
结束时间：27:58
总耗时：21秒
```

#### 场景：单一渠道-elasticsearch。单数据库同步
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 90%
内存：0.5%
开始时间：33:42
结束时间：34:44
总耗时：62秒
```

#### 场景：单一渠道-kafka。单数据库同步
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 230%
内存：1%
开始时间：38:29
结束时间：38:53
总耗时：24秒
```


#### 场景：单一渠道-本地文件。单数据库同步
```
条数：1百万条binlog
批次：1000
资源使用情况
CPU: 280%
内存：0.5%
开始时间：41:48
结束时间：42:26
总耗时：38秒
```
