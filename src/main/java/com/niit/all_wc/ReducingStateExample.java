package com.niit.all_wc;

import com.niit.bean.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ReducingStateExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.socketTextStream("localhost", 7777);

        DataStream<Transaction> transactions = input.map(new MapFunction<String, Transaction>() {
            @Override
            public Transaction map(String value) {
                String[] fields = value.split(",");
                return new Transaction(fields[0], Double.parseDouble(fields[1]));
            }
        });

        transactions.keyBy(transaction -> transaction.userId)
                .process(new KeyedProcessFunction<String, Transaction, String>() {

                    private transient ReducingState<Double> totalAmountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Double> descriptor = new ReducingStateDescriptor<>(
                                "totalAmount",
                                new ReduceFunction<Double>() {
                                    @Override
                                    public Double reduce(Double value1, Double value2) {
                                        return value1 + value2;
                                    }
                                },
                                Double.class
                        );
                        totalAmountState = getRuntimeContext().getReducingState(descriptor);
                    }

                    @Override
                    public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
                        // 更新ReducingState
                        totalAmountState.add(transaction.amount);
                        // 输出当前用户的累积消费金额
                        out.collect("User: " + transaction.userId + ", Total Amount: " + totalAmountState.get());
                    }
                })
                .print();

        env.execute("Flink ReducingState Example");
    }
}

