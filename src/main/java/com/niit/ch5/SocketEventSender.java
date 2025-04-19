package com.niit.ch5;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class SocketEventSender {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9999;

        try (Socket socket = new Socket(host, port);
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected to Flink CEP on port 9999.");
            System.out.println("Enter events in format: username,status (e.g. Alice,fail)");

            while (true) {
                System.out.print("Event> ");
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
                writer.println(line);
            }
//
            System.out.println("Sender closed.");
        } catch (Exception e) {
            System.err.println("Failed to connect: " + e.getMessage());
        }
    }
}
