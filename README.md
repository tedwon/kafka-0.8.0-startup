kafka-0.8.0-startup
===================

Kafka 0.8 Quick Start

http://kafka.apache.org/documentation.html#gettingStarted

Kafka Start Scripts

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic test

bin/kafka-list-topic.sh --zookeeper localhost:2181


