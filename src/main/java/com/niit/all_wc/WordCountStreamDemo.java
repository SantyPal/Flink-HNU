package com.niit.all_wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. creating the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. read data: read from file
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        // TODO 3. Processing Data: Slicing, Transforming, Grouping, Aggregating
        // TODO 3.1 Slicing, Transforming, Grouping, Aggregating
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // split by space
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // Convert to binary (word, 1)
                            Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                            // send data downstream through the collector
                            out.collect(wordsAndOne);
                        }
                    }
                });
        // TODO 3.2 grouping
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        // TODO 3.3 aggregation
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        // TODO 4. output data
        sumDS.print();

        // TODO 5. execution: similar to sparkstreaming last ssc.start()
        env.execute();
    }
}

