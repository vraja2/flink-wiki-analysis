# flink-wiki-analysis
Getting acquainted with Apache Flink through their starter project

If Flink is installed through Homebrew, stdout can be found at:  
`/usr/local/Cellar/apache-flink/1.6.0/libexec/log/flink-user-taskexecutor-0-user.out`

Start zookeeper:  
`./zookeeper-server-start.sh ../config/zookeeper.properties`

Start kafka:  
`./kafka-server-start.sh ../config/server.properties`

Build shadowJar:  
`./gradlew shadowJar`

Run Flink task:  
`flink run -c com.vigneshraja.wikianalysis.WikipediaAnalysis ./build/libs/flink-wiki-analysis-all.jar`

Run Kafka console consumer:  
`./kafka-console-consumer.sh --topic wiki-result --zookeeper localhost:2181`
