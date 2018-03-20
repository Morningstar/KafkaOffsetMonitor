FROM java:8

# Install sbt
RUN echo "deb http://dl.bintray.com/sbt/debian /" >> /etc/apt/sources.list.d/sbt.list && \
    apt-get update && \
    DEBIAN_FRONTEND=nointeractive apt-get --yes --force-yes install sbt

RUN mkdir -p /usr/src/project
RUN mkdir -p /usr/src/app

# Build new jar file
WORKDIR /usr/src/project
COPY . .
RUN sbt assembly && \
    mv /usr/src/project/target/scala-*/KafkaOffsetMonitor-assembly-*.jar /usr/src/app/app.jar && \
    rm -rf /usr/src/project && \
    rm -rf ~/.sbt

# Set environment variables and run
CMD exec java -cp /usr/src/app/app.jar com.quantifind.kafka.offsetapp.OffsetGetterWeb --offsetStorage kafka --port $PORT --kafkaBrokers $KAFKA_BROKERS --zk $ZK --refresh $REFRESH --retain $RETAIN

