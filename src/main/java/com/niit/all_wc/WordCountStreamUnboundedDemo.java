package com.niit.all_wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. create an execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The webui is also visible in the IDEA runtime and is generally used for local testing.
        // A dependency on flink-runtime-web needs to be introduced.
        // Run in IDEA without specifying parallelism, defaults to the number of threads on the computer.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);

        // TODO 2. Read data: socket
        DataStreamSource<String> socketDS = env.socketTextStream("niit", 7777);

        // TODO 3. Processing data: switching, transforming, grouping, aggregating
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING,Types.INT))
//                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(value -> value.f0)
                .sum(1);

        // TODO 4. Output
        sum.print();

        // TODO 5. Execution
        env.execute();
    }
}

