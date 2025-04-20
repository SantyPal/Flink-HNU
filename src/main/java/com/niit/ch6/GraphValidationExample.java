package com.niit.ch6;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.JoinFunction;


public class GraphValidationExample {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //
      //  env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);

        // Step 1: Create Vertices
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "A"),
                new Vertex<>(2L, "B"),
                new Vertex<>(3L, "C"),
                new Vertex<>(4L, "D") // Intentionally no edges for D
        );

        // Step 2: Create Edges (with some invalid cases)
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 1.0),   // valid
                new Edge<>(2L, 3L, 2.0),   // valid
                new Edge<>(3L, 3L, 0.5),   // self-loop
                new Edge<>(4L, 99L, 3.0),  // dangling target (99 doesn't exist)
                new Edge<>(99L, 1L, 1.5),  // dangling source (99 doesn't exist)
                new Edge<>(1L, 2L, 1.0)    // duplicate edge
        );

        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        // === VALIDATION 1: Detect Dangling Edges ===
        DataSet<Long> vertexIds = vertices.map(v -> v.getId());

        // Dangling source
        DataSet<Edge<Long, Double>> danglingSource = edges
                .leftOuterJoin(vertexIds)
                .where(new KeySelector<Edge<Long, Double>, Long>() {
                    @Override
                    public Long getKey(Edge<Long, Double> edge) {
                        return edge.getSource();
                    }
                })
                .equalTo(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long id) {
                        return id;
                    }
                })
                .with(new JoinFunction<Edge<Long, Double>, Long, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Long id) {
                        return id == null ? edge : null;
                    }
                })
                .filter(new FilterFunction<Edge<Long, Double>>() {
                    @Override
                    public boolean filter(Edge<Long, Double> edge) {
                        return edge != null;
                    }
                })
                .returns(new TypeHint<Edge<Long, Double>>() {});

        // Dangling target
        DataSet<Edge<Long, Double>> danglingTarget = edges
                .leftOuterJoin(vertexIds)
                .where(new KeySelector<Edge<Long, Double>, Long>() {
                    @Override
                    public Long getKey(Edge<Long, Double> edge) {
                        return edge.getTarget();
                    }
                })
                .equalTo(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long id) {
                        return id;
                    }
                })
                .with(new JoinFunction<Edge<Long, Double>, Long, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> join(Edge<Long, Double> edge, Long id) {
                        return id == null ? edge : null;
                    }
                })
                .filter(new FilterFunction<Edge<Long, Double>>() {
                    @Override
                    public boolean filter(Edge<Long, Double> edge) {
                        return edge != null;
                    }
                })
                .returns(new TypeHint<Edge<Long, Double>>() {});

        // === VALIDATION 2: Detect Self-loops ===
        DataSet<Edge<Long, Double>> selfLoops = edges
                .filter(edge -> edge.getSource().equals(edge.getTarget()));

        // === VALIDATION 3: Detect Duplicate Edges ===
        DataSet<Tuple3<Long, Long, Integer>> duplicateEdges = edges
                .map(new MapFunction<Edge<Long, Double>, Tuple3<Long, Long, Integer>>() {
                    @Override
                    public Tuple3<Long, Long, Integer> map(Edge<Long, Double> edge) {
                        return new Tuple3<>(edge.getSource(), edge.getTarget(), 1);
                    }
                })
                .groupBy(0, 1)
                .sum(2)
                .filter(new FilterFunction<Tuple3<Long, Long, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<Long, Long, Integer> value) {
                        return value.f2 > 1;
                    }
                });

        // === VALIDATION 4: Detect Isolated Vertices ===
        DataSet<Long> connectedVertexIds = edges
                .flatMap((Edge<Long, Double> edge, Collector<Long> out) -> {
                    out.collect(edge.getSource());
                    out.collect(edge.getTarget());
                })
                .returns(Long.class)
                .distinct();

        DataSet<Vertex<Long, String>> isolatedVertices = vertices
                .leftOuterJoin(connectedVertexIds)
                .where(new KeySelector<Vertex<Long, String>, Long>() {
                    @Override
                    public Long getKey(Vertex<Long, String> vertex) {
                        return vertex.getId();
                    }
                })
                .equalTo(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long id) {
                        return id;
                    }
                })
                .with(new JoinFunction<Vertex<Long, String>, Long, Vertex<Long, String>>() {
                    @Override
                    public Vertex<Long, String> join(Vertex<Long, String> vertex, Long id) {
                        return id == null ? vertex : null;
                    }
                })
                .filter(new FilterFunction<Vertex<Long, String>>() {
                    @Override
                    public boolean filter(Vertex<Long, String> vertex) {
                        return vertex != null;
                    }
                });

        // === Print Results ===
        System.out.println("🔍 Dangling Source Edges:");
        danglingSource.print();

        System.out.println("🔍 Dangling Target Edges:");
        danglingTarget.print();

        System.out.println("🔁 Self-loops:");
        selfLoops.print();

        System.out.println("📑 Duplicate Edges:");
        duplicateEdges.print();

        System.out.println("🚫 Isolated Vertices:");
        isolatedVertices.print();
    }
}
