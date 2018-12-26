#!/usr/bin/env bash
ZOOKEEPER_HOST=zk1.server.com

java -Djava.security.auth.login.config=kafka_client_jaas.conf \
     -Djetty.ssl.keyStore=keystore.jks \
     -Djetty.ssl.keyStorePassword=keypassword \
     -Djetty.ssl.trustStore=truststore.jks \
     -Djetty.ssl.trustStorePassword=trustpassword \
  -cp KafkaOffsetMonitor-assembly-0.4.7-dmilan-SNAPSHOT.jar  \
       com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --zk ${ZOOKEEPER_HOST} \
     --port 8443 \
     --protocol https \
     --refresh 10.seconds \
     --retain 2.days \
     --dbName offsetapp_kafka \
     --consumerConfig /<path>/kafka-config.property