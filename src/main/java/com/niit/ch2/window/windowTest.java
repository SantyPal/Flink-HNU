//package com.niit.ch2.window;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.functions.windowing.ReduceFunction;
//import org.apache.flink.streaming.api.functions.timestamps.SerializableTimestampAssigner;
//
//public class windowTest {
//
//    public static void main(String[] args) throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//
//        // 0.读取数据
//        // nc -lk 7777
//        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
//
//        // 1.先转换成样例数据
//        DataStream<SensorReading> dataStream = inputStream
//                .map(data -> {
//                    String[] arr = data.split(",");
//                    return new SensorReading(arr[0], Long.parseLong(arr[1]), Double.parseDouble(arr[2]));
//                });
//
//        // 每15s统计一次窗口内各传感器所有温度的最小值, 以及最新的时间戳
//        DataStream<Tuple3<String, Double, Long>> resultStream = dataStream
//                .map(data -> new Tuple3<>(data.id, data.temperature, data.timestamp))
//                .keyBy(tuple -> tuple.f0)  // 按照二元组的第一个元素(id)进行分组
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
//                    @Override
//                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> curRes, Tuple3<String, Double, Long> newData) {
//                        return new Tuple3<>(curRes.f0, Math.min(curRes.f1, newData.f1), newData.f2);
//                    }
//                });
//
//        resultStream.print();
//        env.execute("window test");
//    }
//
//    public static class MyReducer implements ReduceFunction<SensorReading> {
//        @Override
//        public SensorReading reduce(SensorReading value1, SensorReading value2) {
//            return new SensorReading(value1.id, value2.timestamp, Math.min(value1.temperature, value2.temperature));
//        }
//    }
//
//    public static class SerializableTimestampAssignerTest implements SerializableTimestampAssigner<SensorReading> {
//        @Override
//        public long extractTimestamp(SensorReading element, long recordTimestamp) {
//            return element.timestamp;
//        }
//    }
//
//    public static class SensorReading {
//        public String id;
//        public long timestamp;
//        public double temperature;
//
//        public SensorReading(String id, long timestamp, double temperature) {
//            this.id = id;
//            this.timestamp = timestamp;
//            this.temperature = temperature;
//        }
//    }
//
//    public static class Tuple3<F0, F1, F2> {
//        public F0 f0;
//        public F1 f1;
//        public F2 f2;
//
//        public Tuple3(F0 f0, F1 f1, F2 f2) {
//            this.f0 = f0;
//            this.f1 = f1;
//            this.f2 = f2;
//        }
//    }
//}
//
