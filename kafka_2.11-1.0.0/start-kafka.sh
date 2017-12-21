#!/bin/bash

nohup bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > /dev/null 2>&1 &
sleep 2
nohup bin/kafka-server-start.sh -daemon config/server.properties > /dev/null 2>&1 &
sleep 2


