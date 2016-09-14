package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

public class RideCleansingKafka {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = TaxiRideOperators.rideSource(env);
        DataStream<TaxiRide> ridesInNYC = TaxiRideOperators.isInNYC(rides);

        ridesInNYC.addSink(new FlinkKafkaProducer09<TaxiRide>(
                "localhost:9092",      // Kafka broker host:port
                "cleansedRides",       // Topic to write to
                new TaxiRideSchema())  // Serializer (provided as util)
        );
        env.execute();
    }
}