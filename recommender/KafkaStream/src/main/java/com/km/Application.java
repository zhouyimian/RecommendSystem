package com.km;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        String input = "abc";
        String output = "recommender";

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "RecommendSystem:9092");
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "RecommendSystem:2181");

        //创建kafkaStream的配置
        StreamsConfig config = new StreamsConfig(properties);

        //建立kafka处理拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source", input)
                .addProcessor("process", () -> new LogProcessor(), "source")
                .addSink("sink", output, "process");

        KafkaStreams kafkaStreams = new KafkaStreams(builder,config);

        kafkaStreams.start();
    }
}
