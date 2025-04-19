package com.niit.ch3.transformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchAggregate {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> products = env.fromElements(
                new Tuple2<>("Laptop", 1500),
                new Tuple2<>("Phone", 800),
                new Tuple2<>("Laptop", 1200),
                new Tuple2<>("Phone", 900)
        );

// GroupBy and Aggregate transformation to sum prices by product
//        DataSet<Tuple2<String, Integer>> totalPrices = products
//                .groupBy(product -> product.f0) // Group by product name
//                .sum(product -> product.f1); // Sum the prices within each group
// Result: [("Laptop", 2700), ("Phone", 1700)]

//        totalPrices.print();
    }
}
