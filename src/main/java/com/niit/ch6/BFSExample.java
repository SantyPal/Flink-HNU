package com.niit.ch6;

import java.util.*;

public class BFSExample {
    public static void main(String[] args) {
        // Create a graph using an adjacency list
        Map<Integer, List<Integer>> graph = new HashMap<>();

        // Add edges (undirected graph)
        addEdge(graph, 0, 1);
        addEdge(graph, 0, 2);
        addEdge(graph, 1, 3);
        addEdge(graph, 2, 4);
        addEdge(graph, 3, 4);
        addEdge(graph, 4, 5);

        // Run BFS from node 0
        System.out.println("BFS traversal starting from node 0:");
        bfs(graph, 0);
    }

    // Function to add an edge to the graph
    public static void addEdge(Map<Integer, List<Integer>> graph, int u, int v) {
        graph.computeIfAbsent(u, k -> new ArrayList<>()).add(v);
        graph.computeIfAbsent(v, k -> new ArrayList<>()).add(u); // for undirected graph
    }

    // BFS function
    public static void bfs(Map<Integer, List<Integer>> graph, int start) {
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> queue = new LinkedList<>();

        queue.offer(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            int current = queue.poll();
            System.out.print(current + " ");

            for (int neighbor : graph.getOrDefault(current, new ArrayList<>())) {
                if (!visited.contains(neighbor)) {
                    queue.offer(neighbor);
                    visited.add(neighbor);
                }
            }
        }
    }
}
