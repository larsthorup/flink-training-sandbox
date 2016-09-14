package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

public class PopularPlacesKafka {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // generate a Watermark every second
        env.getConfig().setAutoWatermarkInterval(1000);

        // configure Kafka consumer
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
        props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
        props.setProperty("group.id", "myGroup");                 // Consumer group ID
        props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

        // create a Kafka consumer
        FlinkKafkaConsumer09<TaxiRide> consumer =
                new FlinkKafkaConsumer09<>(
                        "cleansedRides",
                        new TaxiRideSchema(),
                        props);

        // create Kafka consumer data source
        DataStream<TaxiRide> rides = env.addSource(consumer);

        DataStream<Tuple2<Boolean, Integer>> rideTerminals = TaxiRideOperators.rideTerminals(rides);
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = TaxiRideOperators.popularPlaces(rideTerminals, 30);

        popularPlaces.print();
        env.execute();
    }
}
