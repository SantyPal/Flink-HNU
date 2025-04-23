package com.niit.ch6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

public class test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Integer, String>> ver = env.fromElements(
                new Vertex<>(1, "Santy"),
                new Vertex<>(2, "Phone"),
                new Vertex<>(3, "Tablet"));

        DataSet<Edge<Integer, Double>> edg = env.fromElements(
                new Edge<>(1, 2, 1.0),
                new Edge<>(2, 3, 0.5));

        Graph<Integer, String, Double> graph = Graph.fromDataSet(ver, edg, env);        //graph
       // graph.getVertices().print();
        graph.getEdges().print();
//        DataSet<Integer> VID=ver.map(new MapFunction<Vertex<Integer, String>, Integer>() {
//            @Override
//            public Integer map(Vertex<Integer, String> user_id) throws Exception {
//                return user_id.getId();
//            }
//        });
//        System.out.println("ALL IDs of Vertex");
//        VID.print();
//
//        DataSet<String> VV=ver.map(vertex -> vertex.getValue().toUpperCase());
//        System.out.println("ALL Values of Vertex");
//        VV.print();

       // graph.getEdges().print();
    }
}