package com.niit.all_wc;

import com.niit.bean.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregatingStateExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.socketTextStream("localhost", 7777);

        DataStream<Transaction> transactions = input
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Transaction(fields[0], Double.parseDouble(fields[1]));
                });

        transactions.keyBy(transaction -> transaction.userId)
                .process(new KeyedProcessFunction<String, Transaction, String>() {
                    private transient AggregatingState<Transaction, Double> averageAmountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Transaction, Tuple2<Double, Integer>, Double> descriptor =
                                new AggregatingStateDescriptor<>(
                                        "averageAmount",
                                        new AggregateFunction<Transaction, Tuple2<Double, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Double, Integer> createAccumulator() {
                                                return new Tuple2<>(0.0, 0);
                                            }

                                            @Override
                                            public Tuple2<Double, Integer> add(Transaction value, Tuple2<Double, Integer> accumulator) {
                                                return new Tuple2<>(accumulator.f0 + value.amount, accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Double, Integer> accumulator) {
                                                return accumulator.f0 / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                                                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                                            }
                                        },
                                        Types.TUPLE(Types.DOUBLE,Types.INT)
                                );

                        averageAmountState = getRuntimeContext().getAggregatingState(descriptor);
                    }

                    @Override
                    public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
                        // 更新AggregatingState
                        averageAmountState.add(transaction);
                        // 输出当前用户的平均消费金额
                        out.collect("User: " + transaction.userId + ", Average Amount: " + averageAmountState.get());
                    }
                })
                .print();

        env.execute("Flink AggregatingState Example");
    }
}
