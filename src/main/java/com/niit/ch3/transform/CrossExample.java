package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class CrossExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
      //  env.setParallelism(2);

        // First dataset (IDs and names)
        DataSet<Tuple2<Integer, String>> dataset1 = env.fromElements(
                Tuple2.of(1, "Alice"),
                Tuple2.of(2, "Bob")
        );

        // Second dataset (Departments)
        DataSet<Tuple2<Integer, String>> dataset2 = env.fromElements(
                Tuple2.of(100, "HR"),
                Tuple2.of(200, "IT")
        );

        // Perform Cross transformation = 2*2=4
        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> crossResult = dataset1.cross(dataset2);
       // DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> crossResult = dataset2.cross(dataset1);

        //DS(TP2(<TP2>,<TP2>))

        // Print the result
        System.out.println("Cross Transformation Result:");
        crossResult.print();
    }
}
