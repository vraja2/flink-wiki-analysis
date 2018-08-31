package com.vigneshraja.wikianalysis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Created by vraja on 8/29/18
 */
public class WikipediaAnalysis {

    private static final String TUPLE_OUTPUT_TEMPLATE = "(%s, %d)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(WikipediaEditEvent::getUser);

        DataStream<WikipediaEditEvent> result = keyedEdits
            .timeWindow(Time.seconds(5))
            // using reduce as a hack, can't use "sum" since the WikipediaEditEvent doesn't have a default constructor
            // defined
            .reduce((WikipediaEditEvent e1, WikipediaEditEvent e2) -> new WikipediaEditEvent(
                e1.getTimestamp(),
                e1.getChannel(),
                e1.getTitle(),
                e1.getDiffUrl(),
                e1.getUser(),
                e1.getByteDiff() + e2.getByteDiff(),
                e1.getSummary(),
                e1.isMinor(),
                e1.isNew(),
                e1.isUnpatrolled(),
                e1.isBotEdit(),
                e1.isSpecial(),
                e1.isTalk())
            );

        // Sink to Kafka
        result
            .map((WikipediaEditEvent event) -> String.format(TUPLE_OUTPUT_TEMPLATE, event.getUser(), event.getByteDiff()))
            .addSink(new FlinkKafkaProducer011<>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        see.execute();
    }
}
