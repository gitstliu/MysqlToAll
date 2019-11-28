# MysqlToAll

将mysql数据以指定的格式同步到redis、redis-cluster、kafka、es、本地文件系统的工具

高性能

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
