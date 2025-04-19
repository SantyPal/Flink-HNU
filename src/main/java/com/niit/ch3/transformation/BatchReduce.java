package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class BatchReduce {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> numbers = env.fromElements(1, 2, 3, 4, 5);
// Reduce transformation to sum all elements
        DataSet<Integer> sum = numbers.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) {

                return value1 + value2; //step1, value1=1, value2=2, sum=3
            }                           //step2, value1=sum, value2=3
        });
        sum.print();
    }
}
