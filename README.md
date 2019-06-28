[中文](https://github.com/gitstliu/MysqlToAll/blob/master/README_ch.md)

# MysqlToAll
A Mysql data to  file/elasticsearch/kafka/redis/redis-cluster with binlog tool.

High Performance

# How to config?

## canalconfigs
```
xxx.toml     (canal config file)
xxx.pos      (binlog pos file)
```

## config
```
config.toml (Output source config file)
```
<img  src="https://github.com/gitstliu/MysqlToAll/blob/master/diss_config.png"  alt="具体用法" align=center />


# Performance Test Data

Test Machine : 4C 8G


#### Scene1：Mixed Channels:Tow mysql master binlog write to redis、redis-cluster、kafka、es、local file in one time.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 250%
Mem：3%
Duration：178s
```


#### Scene2：Single Channel:One mysql master binlog write to redis.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 230%
Mem：0.5%
Duration：27s
```

#### Scene3：Single Channel:One mysql master binlog write to redis-cluster.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 230%
Mem：0.5%
Duration：21s
```

#### Scene4：Single Channel:One mysql master binlog write to elasticsearch.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 90%
Mem：0.5%
Duration：62s
```

#### Scene5：Single Channel:One mysql master binlog write to kafka.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 230%
Mem：1%
Duration：24s
```


#### Scene6：Single Channel:One mysql master binlog write to local file.
```
Total Binlog Raw Count：1 Million
Bulksize：              1000
Resource utilization
CPU: 280%
Mem：0.5%
Duration：38s
```

