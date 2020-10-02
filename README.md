# Kafka_SparkStreaming_Cassandra-project



## Overview

The data pipeline read the fake data from our website (due to confidential issue) and sent to Kafka, then transform the data in Spark and store in Cassandra.

Kafka -> Spark Streaming -> Cassandra


## Qucikstart

1. Start a Kafka server
	* create a topic called `portfolio_click`
1. Start a Cassandra database
	* create a keyspace called `payment_space` (SimpleStrategy, replication=1)
		```
		CREATE KEYSPACE paymentspace
		WITH replication = {'class': 'SimpleStrategy, 'replication_factor' : 1};
		```
	* create a table called `paymentclicks` with the following schema
		```
		CREATE TABLE paymentoptions (
			uuid uuid primary key,
			payment text,
			click double
		
		);
	  ```


sbt package && spark-submit --class StreamHandler --master local[*] --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,com.datastax.cassandra:cassandra-driver-core:4.0.0" target/scala-2.12/stream-handler_2.12-1.0.jar
	```
1. From root directory:
	```
	./iot_devices.py payment_frequency
	./iot_devices.py payment_method
	./iot_devices.py first_payment_date
	```
2. `select * from paymentoptions` from CQLSH to see if the data is being processed saved correctly!

![github-small](https://user-images.githubusercontent.com/58568024/94973921-faadae00-04da-11eb-8046-c8f11c494009.png)

This is the screenshot the final result. The number of clicks was groupby the three types of clicks every 5 seconds.


