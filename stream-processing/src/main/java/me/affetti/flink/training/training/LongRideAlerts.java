package me.affetti.flink.training.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 21/09/17.
 */
public class LongRideAlerts {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                60, Time.of(10, TimeUnit.SECONDS)));
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"));

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        DataStream<TaxiRide> rides = env.addSource(
                new CheckpointedTaxiRideSource(input, 6000)
        );

        rides.keyBy(ride -> ride.rideId).process(new LongRides()).print();

        env.execute();
    }

    private static class LongRides extends ProcessFunction<TaxiRide, TaxiRide> {

        private transient ValueState<TaxiRide> rideStart;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<TaxiRide> descriptor = new ValueStateDescriptor<>(
                    "startRides",
                    TypeInformation.of(TaxiRide.class)
            );
            rideStart = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TaxiRide taxiRide, Context context, Collector<TaxiRide> collector) throws Exception {
            if (taxiRide.isStart) {
                long endThreshold = taxiRide.getEventTime() + TimeUnit.HOURS.toMillis(2);
                rideStart.update(taxiRide);
                context.timerService().registerEventTimeTimer(endThreshold);
            } else {
                if (taxiRide.getEventTime() < rideStart.value().getEventTime() + TimeUnit.HOURS.toMillis(2)) {
                    rideStart.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaxiRide> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            TaxiRide start = rideStart.value();
            if (start != null) {
                out.collect(start);
            }
        }
    }
}
