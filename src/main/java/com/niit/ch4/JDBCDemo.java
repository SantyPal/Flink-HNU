package com.niit.ch4;

import java.sql.*;

public class JDBCDemo {
    public static void main(String[] args) {
        // Database URL, username, and password
        String url = "jdbc:mysql://localhost:3306/flink_db"; // Change "testdb" to your database name
        String user = "root"; // Change to your database username
        String password = "123456"; // Change to your database password

        // SQL Query
        String query = "SELECT * FROM users"; // Change "students" to your table name

        // JDBC Connection
        try {
            // 1. Load MySQL JDBC Driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 2. Establish connection
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the database!");
            // 3. Create a Statement
            Statement stmt = conn.createStatement();

            // 4. Execute Query
            ResultSet rs = stmt.executeQuery(query);        //1 row as 1 data
            // 5. Process Results
            while (rs.next()) {
                int id = rs.getInt("id"); // Change column name as needed
                String name = rs.getString("name");
                int age = rs.getInt("age");
                System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
            }

            // 6. Close resources
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
