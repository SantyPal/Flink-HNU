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
        // Set up the Flink streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulated stream of friendships (source, target)
        // Each tuple (Person A, Person B) represents a friendship between A and B
        DataStream<Tuple2<String, String>> friendshipStream = env.fromElements(
                Tuple2.of("Alice", "Bob"),
                Tuple2.of("Alice", "Charlie"),
                Tuple2.of("Bob", "Charlie"),
                Tuple2.of("Charlie", "David"),
                Tuple2.of("David", "Alice")
        );

        // Build adjacency list by grouping edges by the source vertex (person)
        friendshipStream
                .keyBy(friendship -> friendship.f0)  // Group by the source person
                .flatMap(new AdjacencyListBuilder())  // Build adjacency list for each person
                .print();  // Output the adjacency lists to the console

        env.execute("Social Network Adjacency List Builder");
    }

    // This class maintains the adjacency list of friends for each person (source vertex)
    public static class AdjacencyListBuilder extends RichFlatMapFunction<Tuple2<String, String>, String> {

        // State to store the list of neighbors (friends) for each person
        private transient ListState<String> neighbors;

        @Override
        public void open(Configuration parameters) {
            // Initialize the state to store neighbors
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "adjacencyList", String.class);  // Each person will have a list of friends (String)
            neighbors = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, String> friendship, Collector<String> out) throws Exception {
            // Add the target person (friend) to the adjacency list of the source person
            neighbors.add(friendship.f1);  // friendship.f0 is the source (person), friendship.f1 is the friend

            // Collect the list of neighbors for this person
            List<String> neighborList = new ArrayList<>();
            for (String friend : neighbors.get()) {
                neighborList.add(friend);  // Add each friend to the list
            }

            // Emit the adjacency list in a readable format (Person -> List of Friends)
            out.collect("Person " + friendship.f0 + " -> " + neighborList);
        }
    }
}
