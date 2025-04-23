package com.niit.ch6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

public class ver_edg_value {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<Vertex<Long,String>> vertices=env.fromElements(
                new Vertex<>(1L,"A"),
                new Vertex<>(2L,"B"),
                new Vertex<>(3L,"C")
        );

        DataSet<String> VV=vertices.map(new MapFunction<Vertex<Long, String>, String>() {
            @Override
            public String map(Vertex<Long, String> ver_value) throws Exception {
                return ver_value.getValue();
            }
        });
        System.out.println("Vertex Values");
        VV.print();

        DataSet<String> lam_VV=vertices.map(vertex -> vertex.getValue());
        System.out.println("Lambda Vertex Values");
        lam_VV.print();

        DataSet<Long> VID=vertices.map(new MapFunction<Vertex<Long, String>, Long>() {
            @Override
            public Long map(Vertex<Long, String> ID_value) throws Exception {
                return ID_value.getId();
            }
        });
        System.out.println("Vertex ID Values");
        VID.print();

        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.5), // Alice follows Bob
                new Edge<>(2L, 3L, 1.0), // Bob follows Charlie
                new Edge<>(3L, 1L, 0.8)  // Alice follows Charlie
        );
        DataSet<Long> tar_EV=edges.map(new MapFunction<Edge<Long, Double>, Long>() {
            @Override
            public Long map(Edge<Long, Double> value) throws Exception {
                return value.getTarget();
            }
        });
        System.out.println("Edge Target Value");
        tar_EV.print();
        DataSet<Long> sou_EV=edges.map(new MapFunction<Edge<Long, Double>, Long>() {
            @Override
            public Long map(Edge<Long, Double> value) throws Exception {
                return value.getSource();
            }
        });
        System.out.println("Edge Source Value");
        sou_EV.print();

        DataSet<Double> weighr=edges.map((new MapFunction<Edge<Long, Double>, Double>() {
            @Override
            public Double map(Edge<Long, Double> value) throws Exception {
                return value.getValue();            //get the values of weight
            }
        }));
        System.out.println("Edge Weight Value");
        weighr.print();
    }
}
