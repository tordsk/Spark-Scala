#!/usr/bin/env bash
cd kafka/kafka_2.11-0.10.0.1/
nohup bin/zookeeper-server-start.sh config/zookeeper.properties & disown
nohup bin/kafka-server-start.sh config/server.properties & disown
