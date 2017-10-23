package me.affetti.flink.training.enrichment;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 13/08/17.
 */
public class Enrich {
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

        AsyncDataStream.unorderedWait(messages, new RedisRequest(),
                1, TimeUnit.SECONDS, 100).print();

        env.execute();
    }

    private static class RedisRequest extends RichAsyncFunction<String, Tuple2<String, String>> {
        private transient Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jedis = new Jedis(K.REDIS_HOST);
        }

        @Override
        public void close() throws Exception {
            super.close();
            jedis.close();
        }

        @Override
        public void asyncInvoke(String key, AsyncCollector<Tuple2<String, String>> asyncCollector) throws Exception {
            String value = jedis.get(key);
            asyncCollector.collect(Collections.singleton(Tuple2.of(key, value)));
        }
    }
}
