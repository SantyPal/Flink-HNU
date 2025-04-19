package com.niit.ch4.LG;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO read from Kafka: the new source architecture
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // specify the address and port of the kafka node
                .setGroupId("flinkkafka")  // specify the id of the consumer group
                .setTopics("flinkkafka")   // specify the Topic to consume
                .setValueOnlyDeserializer(new SimpleStringSchema()) // specify the deserializer, which deserializes the value.
                .setStartingOffsets(OffsetsInitializer.latest())  // flink's strategy for consuming kafka
                .build();


        DataStreamSource<String> data= env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        data.print();
        env.execute();
    }
}

