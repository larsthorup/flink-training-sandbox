package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PopularPlaces {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = TaxiRideOperators.rideSource(env);
        DataStream<TaxiRide> ridesInNYC = TaxiRideOperators.isInNYC(rides);

        DataStream<Tuple2<Boolean, Integer>> rideTerminals = ridesInNYC.map(new MapFunction<TaxiRide, Tuple2<Boolean, Integer>>() {
            @Override
            public Tuple2<Boolean, Integer> map(TaxiRide ride) throws Exception {
                float lon = ride.isStart ? ride.startLon : ride.endLon;
                float lat = ride.isStart ? ride.startLat : ride.endLat;
                int cellId = GeoUtils.mapToGridCell(lon, lat);
                return new Tuple2<Boolean, Integer>(ride.isStart, cellId);
            }
        });

        KeyedStream<Tuple2<Boolean, Integer>, Tuple> ridesByTerminal = rideTerminals.keyBy(0, 1);

        final int popularityThreshold = 30;

        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces =
                ridesByTerminal
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new Tuple5<Float, Float, Long, Boolean, Integer>(0.0f, 0.0f, Long.MIN_VALUE, false, 0), new FoldFunction<Tuple2<Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {
                    @Override
                    public Tuple5<Float, Float, Long, Boolean, Integer> fold(Tuple5<Float, Float, Long, Boolean, Integer> acc, Tuple2<Boolean, Integer> place) throws Exception {
                        acc.f4 = acc.f4 + 1;
                        return acc;
                    }
                }, new WindowFunction<Tuple5<Float, Float, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple5<Float, Float, Long, Boolean, Integer>> counts, Collector<Tuple5<Float, Float, Long, Boolean, Integer>> out) throws Exception {
                        Integer count = counts.iterator().next().f4;
                        Tuple2<Boolean, Integer> keyTyped = (Tuple2<Boolean, Integer>)key;
                        Boolean isStart = keyTyped.f0;
                        Integer cellId = keyTyped.f1;
                        float lon = GeoUtils.getGridCellCenterLon(cellId);
                        float lat = GeoUtils.getGridCellCenterLat(cellId);
                        Long windowEnd = timeWindow.getEnd();
                        if (count > popularityThreshold) {
                            out.collect(new Tuple5<Float, Float, Long, Boolean, Integer>(lon, lat, windowEnd, isStart, count));
                        }
                    }
                });

        popularPlaces.print();
        env.execute();
    }
}
