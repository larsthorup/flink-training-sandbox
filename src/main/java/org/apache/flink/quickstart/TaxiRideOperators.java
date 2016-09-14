package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by lars_000 on 14-09-2016.
 */
public class TaxiRideOperators {
    public static DataStream<TaxiRide> rideSource(StreamExecutionEnvironment env) {
        int maxDelaySeconds = 60;
        int servingSpeedFactor = 600;
        TaxiRideSource source = new TaxiRideSource("C:\\Users\\lars_000\\Downloads\\nycTaxiRides.gz", maxDelaySeconds, servingSpeedFactor);
        return env.addSource(source);
    }

    public static DataStream<TaxiRide> isInNYC(DataStream<TaxiRide> rides) {
        return rides.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                        && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
        });

    }
}
