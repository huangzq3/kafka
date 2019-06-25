#  kafka demo

1. 下载 Kafka 安装包，然后在本地解压。

2. 启动 Zookeeper

    ```bin/zookeeper-server-start.sh -daemon config/zookeeper.properties```

3. 分别进入kafka安装目录内 执行 

    ```bin/kafka-server-start.sh config/server.properties```
    
 4.打开新的终端，发送消息

    bin/kafka-console-producer.sh --broker-list localhost:9091 --topic errorTopic

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic infoTopic
  
