package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

class IncrementalAverageState {
    public int count = 0;
    public float sum = 0.0f;
};

class ScoreAggregator extends RichFlatMapFunction<Score, Score> {
    private ValueState<IncrementalAverageState> averageState;

    @Override
    public void open(Configuration conf) {
        IncrementalAverageState initialValue = new IncrementalAverageState();
        ValueStateDescriptor<IncrementalAverageState> descriptor = new ValueStateDescriptor<IncrementalAverageState>("averageState", IncrementalAverageState.class, initialValue);
        averageState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Score score, Collector<Score> collector) throws Exception {
        IncrementalAverageState average = averageState.value();
        average.count += 1;
        average.sum += score.value;
        averageState.update(average);
        collector.collect(score);
        Score averageScore = new Score(score.user, "overall", average.sum / average.count); // Note: this assumes that we key by user
        collector.collect(averageScore);
    }
}

public class AggregateScore {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<Score> inputs = env.fromElements(
                new Score("susan", "algebra", 9.0f),
                new Score("susan", "statistics", 3.0f),
                new Score("susan", "europe", 7.0f),
                new Score("susan", "africa", 9.0f),
                new Score("bob", "denmark", 7.0f),
                new Score("bob", "asia", 5.0f)
        );

        DataStream<Score> totals = inputs
                .keyBy("user")
                .flatMap(new ScoreAggregator());


        totals.print();
        env.execute();

        // List<Score> totalList = CollectionUtil.copy(DataStreamUtils.collect(totals));
        // System.out.println(totalList);
        // expect
        // susan math 6
        // susan geography 8
        // susan overall 7
        // bob geography 6
        // bob overall 6
        // overall geography 7
    }
}
