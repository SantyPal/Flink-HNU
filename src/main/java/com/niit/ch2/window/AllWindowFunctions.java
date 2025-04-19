//package com.niit.ch2.window;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.state.KeyedStateStore;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;

//
//import java.time.Duration;
//
//
//public class AllWindowFunctions {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        DataStreamSource<String> input = env.socketTextStream("niit", 9900);
//
//        // ReduceFunction
//        input.keyBy(e -> e)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce(new ReduceFunction<String>() {
//                    public String reduce(String v1, String v2) {
//                        return v1 + "-" + v2;
//                    }
//                })
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner(new SerializableTimestampAssignerTest)
//                )
//                .print();
//
//        // AggregateFunction
//        input.keyBy(e -> e)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .aggregate(new MyAggregateFunction());
//
//        // ProcessWindowFunction
//        input
//                .keyBy(e -> e)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new MyProcessWindowFunction());
//
//        // 增量聚合的 ProcessWindowFunction
//        // 使用 ReduceFunction 增量聚合
//        input
//                .keyBy(e -> e)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce(new MyReduceProcessFunction(), new MyProcessWindowFunction2());
//
//        // 使用 AggregateFunction 增量聚合
//        input
//                .keyBy(e -> e)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .aggregate(new AverageAggregate(), new MyProcessWindowFunction3());
//
//        // 在 ProcessWindowFunction 中使用 per-window state
//        // ProcessWindowFunction
//        input
//                .keyBy(e -> e)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
//                        // 访问全局的 keyed state
//                        KeyedStateStore globalState = context.globalState();
//
//                        // 访问作用域仅限于当前窗口的 keyed state
//                        KeyedStateStore windowState = context.windowState();
//                    }
//                });
//
//        env.execute();
//    }
//}
//class MyAggregateFunction implements AggregateFunction<String, String, String> {
//
//    @Override
//    public String createAccumulator() {
//        return "createAccumulator->";
//    }
//
//    @Override
//    public String add(String s1, String s2) {
//        return s1 + "-" + s2;
//    }
//
//    @Override
//    public String getResult(String s) {
//        return "res=>" + s;
//    }
//
//    @Override
//    public String merge(String s1, String acc1) {
//        return "merge=>" + s1 + ",=>" + acc1;
//    }
//}
//
//class MyProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {
//
//    @Override
//    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
//        for (String res : iterable) {
//            collector.collect(res);
//        }
//    }
//}
//
//class MyReduceProcessFunction implements ReduceFunction<String> {
//
//    public String reduce(String r1, String r2) {
//        return r1 + "-" + r2;
//    }
//}
//
//class MyProcessWindowFunction2 extends ProcessWindowFunction<String, Tuple2<Long, String>, String, TimeWindow> {
//
//    public void process(String key,
//                        Context context,
//                        Iterable<String> minReadings,
//                        Collector<Tuple2<Long, String>> out) {
//        String min = minReadings.iterator().next();
//        out.collect(new Tuple2<>(context.window().getStart(), min));
//    }
//}
//
//class AverageAggregate implements AggregateFunction<String, String, String> {
//
//    @Override
//    public String createAccumulator() {
//        return "createAccumulator=>";
//    }
//
//    @Override
//    public String add(String s1, String s2) {
//        return s1 + "-" + s2;
//    }
//
//    @Override
//    public String getResult(String s) {
//        return s;
//    }
//
//    @Override
//    public String merge(String s, String acc1) {
//        return "merge->" + s + "-" + acc1;
//    }
//}
//
//class MyProcessWindowFunction3 extends ProcessWindowFunction<String, Tuple2<String, Double>, String, TimeWindow> {
//
//    public void process(String key,
//                        Context context,
//                        Iterable<String> averages,
//                        Collector<Tuple2<String, Double>> out) {
//        String average = averages.iterator().next();
//        out.collect(new Tuple2<>(key, 1.0));
//    }
//}