package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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

    public static DataStream<Tuple2<Boolean, Integer>> rideTerminals(DataStream<TaxiRide> rides) {
        return rides.map(new MapFunction<TaxiRide, Tuple2<Boolean, Integer>>() {
            @Override
            public Tuple2<Boolean, Integer> map(TaxiRide ride) throws Exception {
                float lon = ride.isStart ? ride.startLon : ride.endLon;
                float lat = ride.isStart ? ride.startLat : ride.endLat;
                int cellId = GeoUtils.mapToGridCell(lon, lat);
                return new Tuple2<Boolean, Integer>(ride.isStart, cellId);
            }
        });
    }

    public static DataStream<Tuple5<Float,Float,Long,Boolean,Integer>> popularPlaces(DataStream<Tuple2<Boolean, Integer>> rideTerminals, final int popularityThreshold) {

        return rideTerminals
                .keyBy(new KeySelector<Tuple2<Boolean, Integer>, Tuple2<Boolean, Integer>>() {
                    @Override
                    public Tuple2<Boolean, Integer> getKey(Tuple2<Boolean, Integer> input) throws Exception {
                        return new Tuple2<>(input.f0, input.f1);
                    }
                })
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new Tuple5<Float, Float, Long, Boolean, Integer>(0.0f, 0.0f, Long.MIN_VALUE, false, 0), new FoldFunction<Tuple2<Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {
                    @Override
                    public Tuple5<Float, Float, Long, Boolean, Integer> fold(Tuple5<Float, Float, Long, Boolean, Integer> acc, Tuple2<Boolean, Integer> place) throws Exception {
                        acc.f4 = acc.f4 + 1;
                        return acc;
                    }
                }, new WindowFunction<Tuple5<Float, Float, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple2<Boolean, Integer>, TimeWindow>() {
                    @Override
                    public void apply(Tuple2<Boolean, Integer> key, TimeWindow timeWindow, Iterable<Tuple5<Float, Float, Long, Boolean, Integer>> counts, Collector<Tuple5<Float, Float, Long, Boolean, Integer>> out) throws Exception {
                        Integer count = counts.iterator().next().f4;
                        Boolean isStart = key.f0;
                        Integer cellId = key.f1;
                        float lon = GeoUtils.getGridCellCenterLon(cellId);
                        float lat = GeoUtils.getGridCellCenterLat(cellId);
                        Long windowEnd = timeWindow.getEnd();
                        if (count > popularityThreshold) {
                            out.collect(new Tuple5<Float, Float, Long, Boolean, Integer>(lon, lat, windowEnd, isStart, count));
                        }
                    }
                });
    }
}
