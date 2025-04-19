package com.niit.ch3.transform;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class GroupCombineExample {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample dataset (word, count)
        DataSet<Tuple2<String, Integer>> input = env.fromElements(Tuple2.of("apple", 1),        //0
                                                                    Tuple2.of("banana", 2),
                                                                    Tuple2.of("apple", 3),      //2
                                                                    Tuple2.of("banana", 1),
                                                                    Tuple2.of("orange", 5)
        );                                                  //banana=3, apple=4, oranges=5
        // Apply group combine transformation (sum counts by word)
        DataSet<Tuple2<String, Integer>> combined = input
                .groupBy(0)                             //grouping by 1st index elements    //step 1 of grouping the elements (<apple,1),(apple,3)>
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() { //Applies reduceGroup() to sum up the counts of each word.
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
                        int sum = 0;
                        String key = "";
                        for (Tuple2<String, Integer> value : values) {          //actual transformations
                            key = value.f0;                         //f0->apple(after groupBy
                            sum += value.f1;                        //f1->1+3
                        }
                        out.collect(new Tuple2<>(key, sum));        //<apple,4, banana,3
                    }
                });
        // Print the result
        combined.print();
    }
}
