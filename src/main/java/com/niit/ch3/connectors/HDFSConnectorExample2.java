package com.niit.ch3.connectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSConnectorExample2 {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        try { //HDFS configuration
           Configuration conf = new Configuration();  // Configuration object to set HDFS parameters
            conf.set("fs.defaultFS", "hdfs://niit:9000"); // Set HDFS URI
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://niit:9000"), conf, "hdfs");
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // Set HDFS implementation
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());     // Set LocalFileSystem implementation

            // Create a simple dataset
           String hdfsInputPath = "hdfs://niit:9000/flink/wordcount.txt";      //source file form hdfs
            DataSet<String> textData = env.readTextFile(hdfsInputPath);         //coping the data into dataset

        //    DataSet<String> textData=env.readTextFile("hdfs://niit:9000/flink/wordcount.txt");
            DataSet<String> processedData = textData.map(new MapFunction<String, String>() {
                @Override
                public String map(String value) {
                    return value.toUpperCase();
                }
            });

            // Write processed data back to HDFS
            String hdfsOutputPath = "hdfs://niit:9000/flink_output/processed_data.txt";
            processedData.writeAsText(hdfsOutputPath);


        } catch (URISyntaxException | IOException | InterruptedException e) {
            System.err.println("An error occurred while copying the file to HDFS:");
            e.printStackTrace();
        }
        // Execute the job
        env.execute("HDFS Write Example");
    }
}
