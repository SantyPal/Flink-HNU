package com.niit.ch3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import java.util.List;

public class FlinkBroadcastExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Small dataset to broadcast (id, category)
        DataSet<Tuple2<Integer, String>> categories = env.fromElements(     //
                Tuple2.of(1, "Electronics"),
                Tuple2.of(2, "Clothing"),
                Tuple2.of(3, "Groceries")

        );
        // Main dataset with (id, product name)
        DataSet<Tuple2<Integer, String>> products = env.fromElements(
                Tuple2.of(1, "Laptop"),
                Tuple2.of(2, "Shirt"),
                Tuple2.of(3, "Apple"),
                Tuple2.of(3, "Banana"),
                Tuple2.of(2, "Trouser"),
                Tuple2.of(4, "Shoes"),
                Tuple2.of(4, "Slipperrs"),
                Tuple2.of(1, "Phone")
        );

        // Apply broadcast variable
        DataSet<String> result = products.map(new RichMapFunction<Tuple2<Integer, String>, String>() {      //broadcast transformation
            private List<Tuple2<Integer, String>> categoryList;         //create a broadcast variable for LIST
            //1. electronic             not the dataset element
            //2.

            @Override
            public void open(Configuration parameters) {                                        //open method to initialize the variable to get the broadcast
                this.categoryList = getRuntimeContext().getBroadcastVariable("categories");
            }

            @Override
            public String map(Tuple2<Integer, String> product) {                //transformation one - to one element comparison
                for (Tuple2<Integer, String> category : categoryList) {
                    if (category.f0.equals(product.f0)) {                       //comparison
                        return product.f1 + " belongs to " + category.f1;
                    }
                }
                return product.f1 + " belongs to Unknown Category";
            }
        }).withBroadcastSet(categories, "categories");

        // Print result
        result.print();
       // env.execute();
    }
}