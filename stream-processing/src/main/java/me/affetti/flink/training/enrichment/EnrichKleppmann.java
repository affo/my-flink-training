package me.affetti.flink.training.enrichment;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 13/08/17.
 */
public class EnrichKleppmann {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(0);
        env.setParallelism(4);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                60, Time.of(10, TimeUnit.SECONDS)));
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", K.KAFKA_HOST + ":9092");
        properties.setProperty("group.id", "test");

        DataStream<String> messages = env.addSource(
                new FlinkKafkaConsumer010<>(
                        "test", new SimpleStringSchema(), properties
                ));

        FlinkKafkaConsumer010<String> updatesConsumer = new FlinkKafkaConsumer010<>(
                "updates", new SimpleStringSchema(), properties
        );
        // replay the updates from the beginning
        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("updates", 0), 0L);
        updatesConsumer.setStartFromSpecificOffsets(specificStartOffsets);
        DataStream<Tuple2<String, String>> updates = env.addSource(updatesConsumer)
                .map(s -> {
                    String[] tokens = s.split(":");
                    return Tuple2.of(tokens[0], tokens[1]);
                })
                .returns(new TypeHint<Tuple2<String, String>>() {
                });


        DataStream<Tuple2<String, String>> out = messages.connect(updates)
                .keyBy(
                        new KeySelector<String, String>() {
                            @Override
                            public String getKey(String s) throws Exception {
                                return s;
                            }
                        },
                        new KeySelector<Tuple2<String, String>, String>() {
                            @Override
                            public String getKey(Tuple2<String, String> t) throws Exception {
                                return t.f0;
                            }
                        }
                )
                .flatMap(new Enricher());

        out.addSink(new InfluxDBSink<>()).setParallelism(1);
        out.print();

        env.execute();
    }

    private static class Enricher extends RichCoFlatMapFunction<String, Tuple2<String, String>, Tuple2<String, String>> {
        private transient ValueState<String> values;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<String> sd = new ValueStateDescriptor<>("values", String.class);
            values = getRuntimeContext().getState(sd);
        }

        /**
         * From Kafka topic "test"
         * @param key
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap1(String key, Collector<Tuple2<String, String>> collector) throws Exception {
            collector.collect(Tuple2.of(key, values.value()));
        }

        /**
         * From Kafka topic "updates"
         * @param entry
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap2(Tuple2<String, String> entry,
                             Collector<Tuple2<String, String>> collector) throws Exception {
            values.update(entry.f1);
        }
    }
}
