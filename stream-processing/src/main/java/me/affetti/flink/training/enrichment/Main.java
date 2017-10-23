package me.affetti.flink.training.enrichment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Created by affo on 13/08/17.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(0);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", K.KAFKA_HOST + ":9092");
        properties.setProperty("group.id", "test");

        DataStream<String> messages = env.addSource(
                new FlinkKafkaConsumer010<>(
                        "test", new SimpleStringSchema(), properties
                ));

        messages.map(String::toUpperCase).print();

        env.execute();
    }
}
