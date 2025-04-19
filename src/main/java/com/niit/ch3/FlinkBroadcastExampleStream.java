package com.niit.ch3;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;


public class FlinkBroadcastExampleStream {
    public static void main(String[] args) throws Exception {
       // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // Small dataset to broadcast (id, category)
        DataStream<Tuple2<Integer, String>> categories = env.fromElements(
                Tuple2.of(1, "Electronics"),
                Tuple2.of(2, "Clothing"),
                Tuple2.of(3, "Groceries")
        );

        /* Main dataset with (id, product name)
        DataStream<Tuple2<Integer, String>> products = env.fromElements(
                Tuple2.of(1, "Laptop"),
                Tuple2.of(2, "Shirt"),
                Tuple2.of(3, "Apple"),
                Tuple2.of(3, "Banana"),
                Tuple2.of(2, "Trouser"),
                Tuple2.of(4, "Shoes")
        );*/
        DataStreamSource<String> productSource = env.socketTextStream("niit", 9900);

        // Parse incoming product data
        DataStream<Tuple2<Integer, String>> products = productSource.map(line -> {
            String[] parts = line.split(",");
            return Tuple2.of(Integer.parseInt(parts[0].trim()), parts[1].trim());
        }).returns(Types.TUPLE(Types.INT, Types.STRING));

        // Define a broadcast state descriptor
        MapStateDescriptor<Integer, String> categoryStateDescriptor = new MapStateDescriptor<>(
                "categoriesBroadcastState",
                Types.INT,
                Types.STRING
        );

        // Create a broadcast stream
        BroadcastStream<Tuple2<Integer, String>> broadcastCategories = categories.broadcast(categoryStateDescriptor);

        // Connect main data stream with broadcast stream and process
        DataStream<String> result = products.connect(broadcastCategories)
                .process(new BroadcastProcessFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    private transient MapStateDescriptor<Integer, String> stateDescriptor;

                    @Override
                    public void open(Configuration parameters) {
                        stateDescriptor = new MapStateDescriptor<>("categoriesBroadcastState", Types.INT, Types.STRING);
                    }

                    @Override
                    public void processElement(Tuple2<Integer, String> product, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        String category = Optional.ofNullable(ctx.getBroadcastState(stateDescriptor).get(product.f0)).orElse("Unknown Category");
                        out.collect(product.f1 + " belongs to " + category);
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> category, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        broadcastState.put(category.f0, category.f1);
                    }
                });

        // Print result
        result.print();

        env.execute("Flink Broadcast Example");
    }
}
