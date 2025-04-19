package com.niit.ch3.transform;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class testTransformations {
    public static void main(String[] args) throws Exception {
       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String,Integer> > input = env.fromElements(Tuple2.of("santy",12),
                                                                            new Tuple2<>("Phone",11),
                                                                            new Tuple2<>("Tablet",13),
                                                                            new Tuple2<>("santy",23),
                                                                            new Tuple2<>("Phone",21),
                                                                            new Tuple2<>("Tablet",23));

        DataSet<Tuple2<String, Integer>> sumoutput = input.groupBy(0).sum(1);
        DataSet<Tuple2<String, Integer>> maxoutput = input.groupBy(0).maxBy(1);
        DataSet<Tuple2<String, Integer>> minoutput = input.groupBy(0).minBy(1);
        sumoutput.print();
        System.out.println("-----------------------------------------------------------");
        maxoutput.print();
        System.out.println("-----------------------------------------------------------");
        minoutput.print();
    }
}
