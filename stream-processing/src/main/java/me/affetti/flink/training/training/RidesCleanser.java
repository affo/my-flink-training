package me.affetti.flink.training.training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by affo on 21/09/17.
 */
public class RidesCleanser implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide ride) throws Exception {
        return GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat);
    }
}
