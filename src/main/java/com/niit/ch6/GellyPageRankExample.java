package com.niit.ch6;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import java.util.ArrayList;

import java.util.List;

public class GellyPageRankExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulated stream of edges (source, target)
        DataStream<Tuple2<Long, Long>> edgeStream = env.fromElements(
                Tuple2.of(1L, 2L),
                Tuple2.of(1L, 3L),
                Tuple2.of(2L, 3L),
                Tuple2.of(3L, 4L),
                Tuple2.of(4L, 1L)
        );

        // Build adjacency list keyed by source vertex
        edgeStream
                .keyBy(edge -> edge.f0)
                .flatMap(new AdjacencyListBuilder())
                .print();

        env.execute("Stream Graph Builder");
    }

    // Maintains an adjacency list per source node
    public static class AdjacencyListBuilder extends RichFlatMapFunction<Tuple2<Long, Long>, String> {

        private transient ListState<Long> neighbors;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "adjacencyList", Long.class);
            neighbors = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> edge, Collector<String> out) throws Exception {
            neighbors.add(edge.f1);

            List<Long> neighborList = new ArrayList<>();
            for (Long n : neighbors.get()) {
                neighborList.add(n);
            }

            out.collect("Vertex " + edge.f0 + " -> " + neighborList);
        }

    }
}
