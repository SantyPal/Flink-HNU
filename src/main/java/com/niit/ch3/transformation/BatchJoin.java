package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchJoin {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //how many elements in this dataset=3
        DataSet<Tuple2<Integer, String>> orders = env.fromElements(new Tuple2<>(1, "Laptop"),
                                                                    new Tuple2<>(2, "Phone"),
                                                                    new Tuple2<>(3, "Tablet"));

        DataSet<Tuple2<Integer, Double>> prices = env.fromElements(new Tuple2<>(1, 1500.0),
                                                                    new Tuple2<>(2, 800.0));
// Join transformation to combine orders with their prices
        DataSet<Tuple3<Integer, String, Double>> orderWithPrices = orders.join(prices)          //we are joining 2 datasets
                .where(order -> order.f0) // Key for the first DataSet (orders) == 1
                .equalTo(price -> price.f0) // Key for the second DataSet (prices) ==1
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, Double>, Tuple3<Integer, String, Double>>() {
                    @Override
                    public Tuple3<Integer, String, Double> join(Tuple2<Integer, String> order, Tuple2<Integer, Double> price) {
                        return new Tuple3<>(order.f0, order.f1, price.f1);
                    }
                });
        orderWithPrices.print();
    }
}
