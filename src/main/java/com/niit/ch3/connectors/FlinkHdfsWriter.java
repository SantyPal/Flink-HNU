//package com.niit.ch3.connectors;
//
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.hadoop.conf.Configuration;
//
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//
//
//public class FlinkHdfsWriter {
//    public static void main(String[] args) throws Exception {
//        // 1️⃣ Set up the execution environment
//       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 2️⃣ Create a data stream from a socket source (e.g., Netcat)
//        String hostname = "niit";
//        int port = 9900;
//        DataStream<String> dataStream = env.addSource(new SocketTextStreamFunction(hostname, port, "\n", 0));
//
//        try {
//            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://niit:9000"); // Set HDFS URI
//            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://niit:9000"), conf, "hdfs");
//            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        // 3️⃣ Define the HDFS Sink
//        final StreamingFileSink<String> hdfsSink = StreamingFileSink
//                .forRowFormat(new Path("hdfs://niit:9000/flink/output/"), new SimpleStringEncoder<String>("UTF-8"))
//                .build();
//
//        // 4️⃣ Write stream to HDFS
//        dataStream.addSink(hdfsSink);
//        } catch (URISyntaxException | IOException | InterruptedException e) {
//            System.err.println("An error occurred while copying the file to HDFS:");
//            e.printStackTrace();
//        }
//        // 5️⃣ Execute the Flink job
//        env.execute("Flink HDFS Writer Example");
//    }
//}
//
