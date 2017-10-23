package me.affetti.flink.training.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Created by affo on 28/09/17.
 */
public class LongRidesCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(
                new CheckpointedTaxiRideSource("nycTaxiRides.gz", 600)
        ).keyBy(r -> r.rideId);

        Pattern<TaxiRide, TaxiRide> pattern = Pattern.<TaxiRide>begin("start")
                .where(
                        new SimpleCondition<TaxiRide>() {
                            @Override
                            public boolean filter(TaxiRide taxiRide) throws Exception {
                                return taxiRide.isStart;
                            }
                        }
                )
                .next("end")
                .where(
                        new SimpleCondition<TaxiRide>() {
                            @Override
                            public boolean filter(TaxiRide taxiRide) throws Exception {
                                return !taxiRide.isStart;
                            }
                        }
                )
                .within(Time.hours(2));

        PatternStream<TaxiRide> patternStream = CEP.pattern(rides, pattern);
        patternStream
                .flatSelect(
                        new PatternFlatTimeoutFunction<TaxiRide, TaxiRide>() {
                            @Override
                            public void timeout(Map<String, List<TaxiRide>> map, long l,
                                                Collector<TaxiRide> collector) throws Exception {
                                TaxiRide startRide = map.get("start").get(0);
                                collector.collect(startRide);
                            }
                        },
                        new PatternFlatSelectFunction<TaxiRide, TaxiRide>() {
                            @Override
                            public void flatSelect(Map<String, List<TaxiRide>> map,
                                                   Collector<TaxiRide> collector) throws Exception {
                                // does nothing
                            }
                        }
                )
                .print();

        env.execute();
    }
}
