//package com.niit.ch3.connectors;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.hadoop.conf.Configuration;
//
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//
//public class HDFSConnectorExample {
//    public static void main(String[] args) throws Exception {
//        // Set up the execution environment
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        try {
//            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://niit:9000"); // Set HDFS URI
//            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://niit:9000"), conf, "hdfs");
//            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//       // FileSystem.initialize(hdfs, env.getConfig());
//
//        // Create a simple dataset
//        env.fromElements("Hello", "World")
//                .map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String value) {
//                        return Tuple2.of(value, 1);
//                    }
//                })
//                .writeAsText("hdfs://niit:9000/flink_output/wc.txt");
//        } catch (URISyntaxException | IOException | InterruptedException e) {
//            System.err.println("An error occurred while copying the file to HDFS:");
//            e.printStackTrace();
//        }
//        // Execute the job
//        env.execute("HDFS Write Example");
//    }
//}
