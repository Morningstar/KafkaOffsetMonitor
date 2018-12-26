Kafka Offset Monitor
===========

[![Build Status](https://travis-ci.org/Morningstar/kafka-offset-monitor.svg?branch=master)](https://travis-ci.org/Morningstar/kafka-offset-monitor)

This is an app to monitor your kafka consumers and their position (offset) in the log.

You can see the current consumer groups, for each group the topics that they are consuming and the position of the group in each topic log. This is useful to understand how quick you are consuming from a log and how fast the log is growing. It allows for debuging kafka producers and consumers or just to have an idea of what is going on in your system.

The app keeps a history of log position and lag of the consumers so you can have an overview of what has happened in the last days. The amount of history to keep is definable at runtime.

Here are a few screenshots:

List of Consumer Groups
-----------------------

![Consumer Groups](http://quantifind.github.io/KafkaOffsetMonitor/img/groups.png)

List of Topics for a Group
--------------------------

![Topic List](http://quantifind.github.io/KafkaOffsetMonitor/img/topics.png)

History of Topic position
-------------------------

![Position Graph](http://quantifind.github.io/KafkaOffsetMonitor/img/graph.png)

Offset Types
===========

Kafka is flexible on how the offsets are managed. Consumer can choose arbitrary storage and format to persist offsets.  KafkaOffsetMonitor currently 
supports following popular storage formats

* kafka built-in offset management API (based on broker metadata and Kafka's own internal __consumer_offsets topic)
* zookeeper built-in high-level consumer (based on Zookeeper)
* Storm Kafka Spout (based on Zookeeper by default)

Each runtime instance of KafkaOffsetMonitor can only support a single type of storage format.

Building It
===========

The command below will build a fat-jar in the target/scala_${SCALA_VERSION} directory which can be run by following the command in the "Running It" section.

```bash
$ sbt clean assembly
```

Running It
===========

If you do not want to build it manually, just download the [current jar](https://github.com/dmilan77/kafka-offset-monitor/releases/latest).

This is a small web app, you can run it locally or on a server, as long as you have access to the Kafka broker(s) and ZooKeeper nodes storing kafka data.

```
# For http 
java -Djava.security.auth.login.config=conf/server-client-jaas.conf \
	-cp KafkaOffsetMonitor-assembly-0.4.6-SNAPSHOT.jar \
       com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --offsetStorage kafka \
     --zk zkserver01,zkserver02 \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days \
     --dbName offsetapp_kafka \
     --consumerConfig  consumerConfig-File
     
# For https
java -Djava.security.auth.login.config=conf/server-client-jaas.conf \
	-cp KafkaOffsetMonitor-assembly-0.4.6-SNAPSHOT.jar \
   -Djetty.ssl.keyStore=/<path-to-keystore>/localhost-keystore.jks \
    -Djetty.ssl.keyStorePassword=trustme \
    -Djetty.ssl.trustStore=/<path-to-trusstore>/all-truststore.jks \
    -Djetty.ssl.trustStorePassword=trustme \ 
       com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --offsetStorage kafka \
     --protocol https \
     --zk zkserver01,zkserver02 \
     --port 8080 \
     --refresh 10.seconds \
     --retain 2.days \
     --dbName offsetapp_kafka \
     --consumerConfig  consumerConfig-File

```

The arguments are:

- **offsetStorage** valid options are ''kafka'', ''zookeeper'', or ''storm''. Anything else falls back to ''zookeeper''
- **zk** the ZooKeeper hosts
- **consumerConfig** kafka consumer config. Needed for SSL/TLS Kafka
- **port** the port on which the app will be made available
- **protocol** `http` or `https` . Default value `http`
- **refresh** how often should the app refresh and store a point in the DB
- **retain** how long should points be kept in the DB
- **dbName** where to store the history (default 'offsetapp')
- **kafkaOffsetForceFromStart** only applies to ''kafka'' format. Force KafkaOffsetMonitor to scan the commit messages from start (see notes below)
- **stormZKOffsetBase** only applies to ''storm'' format.  Change the offset storage base in zookeeper, default to ''/stormconsumers'' (see notes below)
- **pluginsArgs** additional arguments used by extensions (see below)

Special Notes on Kafka Offset Storage
======================================

As of Kafka v0.9, Kafka's built-in offset management saves offsets in an internal topic ''__consumer_offsets'' as ''commit''
messages. Throughout running, we request a list of consumers from Kafka brokers which returns a list of all consumers
Kafka has ever seen. Because there is no place to directly query for ''active'' consumers, KafkaOffsetMonitor needs to
''discover'' active consumers by reading those ''commit'' messages. If consumers are active, KafkaOffsetMonitor will
read ''commit'' messages from those active consumers and they will be ''discovered'' after a short while.

Special Notes on Storm Storage
===============================

By default, Storm Kafka Spout stores offsets in ZK in a directory specified via ''SpoutConfig''. At same time, Kafka also stores its meta-data inside zookeeper. 
In order to monitor Storm Kafka Spout offsets, KafkaOffsetMonitor requires that:
 
 * Spout and Kafka use the same zookeeper cluster
 * Spout stores the offsets under a sub-directory of Kafka's meta-data directory 

This sub-directory can be configured via ''stormZKOffsetBase''. The default value is ''/stormconsumers''

Writing and using plugins
============================

Kafka Offset Monitor allows you to plug-in additional offset info reporters in case you want this information to be logged or stored somewhere. In order to write your own plugin,
all you need to do is to implement OffsetInfoReporter trait:

```
trait OffsetInfoReporter {
  def report(info: IndexedSeq[OffsetInfo])
  def cleanupOldData() = {}
}
```

It is also required, that implementation has a constructor with String as the only parameter, and this parameter will be set to pluginsArgs argument value.
Its up to you how you want to utilize this argument and configure your plugin.

When building a plugin you may find it difficult to set up dependency to Kafka Offset Monitor classes, as currently artifacts are not published to public repos.
As long as this is true you will need to use local maven repo and just publish Kafka Offset Monitor artifact with: ```sbt publishM2```

Assuming you have a custom implementation of OffsetInfoReporter in a jar file, running it is as simple as adding the jar to the classpath when running app:

```
java -Djava.security.auth.login.config=/<path>/kafka_client_jaas.conf \
  -cp KafkaOffsetMonitor-assembly-0.4.7-dmilan-SNAPSHOT.jar  \
       com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --zk ${MASTER1} \
     --port 8089 \
     --refresh 10.seconds \
     --retain 2.days \
     --dbName offsetapp_kafka \
     --consumerConfig /<path-to-jaas>/kafka-config.property
```


Contributing
============

The KafkaOffsetMonitor is released under the Apache License and we **welcome any contributions** within this license. Any pull request is welcome and will be reviewed and merged as quickly as possible.
