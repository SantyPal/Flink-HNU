package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class BatchMap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements("Apache Flink", "Batch Processing");
// Map transformation to convert text to upper case

        DataSet<String> upperCaseText = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        });
        upperCaseText.print();          //stdout = sink

       // upperCaseText.writeAsText("src/main/resources/output.txt");
      //  upperCaseText.writeAsText("src/main/resources/output.txt", FileSystem.WriteMode.OVERWRITE);
        upperCaseText.writeAsCsv("src/main/resources/output.csv");
        env.execute();
    }
}
