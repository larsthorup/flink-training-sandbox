package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansing {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int maxDelaySeconds = 60;
        int servingSpeedFactor = 600;
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("C:\\Users\\lars_000\\Downloads\\nycTaxiRides.gz", maxDelaySeconds, servingSpeedFactor));

        DataStream<TaxiRide> ridesInNYC = rides.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                        && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
        });

        ridesInNYC.print();
        env.execute();
    }
}