package com.niit.ch4;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import java.util.Arrays;
import java.util.List;

public class FlinkMySQLTableAPI_Stream {
    public static void main(String[]args)throws Exception {
        // ✅ Step 1: Set up Flink Table Environment in Streaming Mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode() // Enable streaming mode
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // ✅ Step 2: Create a MySQL Table (Persistent Sink)
        TableResult tr = tableEnv.executeSql(
                "CREATE TABLE student_scores (id INT PRIMARY KEY NOT ENFORCED, name STRING) WITH ('connector' = 'jdbc'," +
                        " 'url' = 'jdbc:mysql://localhost:3306/flink_db','table-name' = 'student_scores', 'driver' = 'com.mysql.cj.jdbc.Driver'," +
                        " 'username' = 'root','password' = '123456')"
        );
        tr.print();

        // ✅ Step 3: Insert Streaming Data using POJO Class
        List<Student> studentData = Arrays.asList(
                new Student(1, "Alice"),
                new Student(2, "Bob"),
                new Student(3, "Charlie")
        );

        Table studentTable = tableEnv.fromValues(
                studentData.stream()
                        .map(student -> Row.of(student.getId(), student.getName())).toArray(Row[]::new)
        );
        tableEnv.createTemporaryView("students_view", studentTable);

        // ✅ Step 4: Insert Data into MySQL (Streaming Sink)
        tableEnv.executeSql("INSERT INTO student_scores SELECT * FROM students_view");
        tableEnv.executeSql("SELECT * FROM student_scores").print();
    }
}
// ✅ POJO Class for Student
class Student {
    private int id;
    private String name;

    public Student(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}