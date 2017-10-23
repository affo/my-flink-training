package me.affetti.flink.training.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 21/09/17.
 * <p>
 * The task of the “Taxi Ride Cleansing” exercise is to cleanse a stream of TaxiRide events by removing events that do not start or end in New York City.
 * The GeoUtils utility class provides a static method isInNYC(float lon, float lat) to check if a location is within the NYC area
 */
public class TaxiRideCleansing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", 60, 600)
        );
        rides
                .filter(new RidesCleanser())
                .print();

        env.execute();
    }
}
