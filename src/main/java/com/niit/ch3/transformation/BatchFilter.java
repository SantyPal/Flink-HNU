package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class BatchFilter {
    public static void main(String[] args) throws Exception {
        //set the flink Environment
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        //load the data (file, custom data)
        DataSet<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6);          //loading the data

// Filter transformation to retain only even numbers
        DataSet<Integer> evenNumbers = numbers.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer%2==0;
            }
        });
        evenNumbers.print();
    }
}
