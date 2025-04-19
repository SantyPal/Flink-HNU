package com.niit.ch5;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleSocketServer {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(9999);
        System.out.println("✅ Server running on port 9999...");
        Socket client = server.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println("📥 RECEIVED: " + line);
        }
    }
}
