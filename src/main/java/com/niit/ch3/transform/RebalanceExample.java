package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class RebalanceExample {
    public static void main(String[] args) throws Exception {

        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();            //task 1
        env.setParallelism(2);                                                              //4 Tasks

        // Create a dataset with skewed data
        DataSet<Integer> input = env.fromElements(1, 1, 1, 2, 3, 4, 5, 6, 7, 8);

        // Without rebalance transformation
        DataSet<Integer> rebalancedData = input.rebalance();


        // Apply rebalance transformation
       // Print the result (execution required in a real distributed environment)
        rebalancedData.print();
    }

}
