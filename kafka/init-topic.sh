#!/bin/bash

echo "Waiting for Kafka..."

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list > /dev/null 2>&1
do
    sleep 2
done

echo "Kafka is ready."

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic telecom_logs --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

echo "Topic initialization complete."