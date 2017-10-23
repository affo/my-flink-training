package me.affetti.flink.training.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 21/09/17.
 */
public class PopularPlaces {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final int popularityThreshold = 20;

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", 60, 600)
        );
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popular = rides
                .filter(new RidesCleanser())

        /* we could split but this will make us double the code...
                .split((OutputSelector<TaxiRide>) taxiRide ->
                        taxiRide.isStart ? Collections.singletonList("start") : Collections.singletonList("end"));

        DataStream<TaxiRide> starts = splitted.select("start");
        DataStream<TaxiRide> ends = splitted.select("end");
        */

                .map(new ToGridAndStart())
                // we key by start and gridID
                .keyBy(0, 1)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new PopularityCalculator())
                .filter(t -> t.f4 > popularityThreshold);

        popular.print();

        env.execute();
    }

    private static class ToGridAndStart implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {
        @Override
        public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
            Integer gridId = taxiRide.isStart ? GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat) :
                    GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            return Tuple2.of(gridId, taxiRide.isStart);
        }
    }

    private static class PopularityCalculator implements WindowFunction<
            Tuple2<Integer, Boolean>, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<Integer, Boolean>> iterable,
                          Collector<Tuple5<Float, Float, Long, Boolean, Integer>> collector) throws Exception {
            int count = 0;

            for (Tuple2<Integer, Boolean> t : iterable) {
                count++;
            }

            Float longitutude = GeoUtils.getGridCellCenterLon(key.getField(0));
            Float latitude = GeoUtils.getGridCellCenterLat(key.getField(0));
            collector.collect(Tuple5.of(longitutude, latitude, timeWindow.getEnd(), key.getField(1), count));
        }
    }
}
