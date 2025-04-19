package com.niit.all_wc;

import com.niit.bean.WaterSensor;
import com.niit.ch3.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class ProcessAllWindowTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );


        // Last 10 seconds = window length, output every 5 seconds = sliding step.
        // TODO all data together, store in hashmap, key=vc, value=count value
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyTopNPAWF())
                .print();


        env.execute();
    }


    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            // Define a hashmap to store the key=vc and value=count values.
            Map<Integer, Integer> vcCountMap = new HashMap<>();
            // 1. Iterate through the data, counting the number of times each vc appears.
            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                if (vcCountMap.containsKey(vc)) {
                    // 1.1 key exists, not the first piece of data for this key, just accumulate it
                    vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                } else {
                    // 1.2 key does not exist, initialize
                    vcCountMap.put(vc, 1);
                }
            }

            // 2. Sorting the count values: Sorting with Lists
            List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc, vcCountMap.get(vc)));
            }
            // Sort the list, descending by count.
            datas.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    // Descending, back minus front
                    return o2.f1 - o1.f1;
                }
            });

            // 3. Take the two vc's with the highest count.
            StringBuilder outStr = new StringBuilder();

            outStr.append("================================\n");
            // Iterate through the sorted List and take out the first 2, considering the possibility that there are not enough lists == “Number of elements in the List and 2, min.
            for (int i = 0; i < Math.min(2, datas.size()); i++) {
                Tuple2<Integer, Integer> vcCount = datas.get(i);
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc=" + vcCount.f0 + "\n");
                outStr.append("count=" + vcCount.f1 + "\n");
                outStr.append("Window end time =" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
                outStr.append("================================\n");
            }

            out.collect(outStr.toString());


        }
    }
}

