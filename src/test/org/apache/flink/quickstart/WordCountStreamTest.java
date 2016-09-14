package org.apache.flink.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.Console;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class WordCountStreamTest {


    public static HashMap<String, Integer> map(List<Tuple2<String, Integer>> input) {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (int i = 0; i < input.size(); ++i) {
            Tuple2<String, Integer> tup = input.get(i);
            result.put(tup.f0, tup.f1);
        }
        return result;
    }

    @Test
    public void transform() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements(
            "Hey Lars",
            "Hey Sju"
        );

        DataStream<Tuple2<String, Integer>> counts = WordCountStream.transform(text);

        List<Tuple2<String, Integer>> countList = CollectionUtil.copy(DataStreamUtils.collect(counts));
        HashMap<String, Integer> countResult = map(countList);
        // System.out.println(countResult);
        assertEquals((Integer)2, countResult.get("hey"));
        assertEquals((Integer)1, countResult.get("lars"));
        assertEquals((Integer)1, countResult.get("sju"));
    }

}