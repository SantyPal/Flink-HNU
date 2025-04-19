package com.niit.ch4;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import java.util.Arrays;
import java.util.List;

public class FlinkMySQLTableAPI {
    public static void main(String[] args) throws Exception {
        // ✅ Step 1: Set up Flink Table Environment in Batch Mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()    //will work for batch processing or stream processing
                .inBatchMode()  // Enable batch processing
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        tableEnv.executeSql("DROP TABLE IF EXISTS students");
        // ✅ Step 2: Create a Temporary Table in Flink (Equivalent to Tuple2)
        // ✅ 2. Create MySQL Table (Persistent Sink)
        TableResult tr= tableEnv.executeSql(
                "CREATE TABLE student_scores (" +
                        " id INT PRIMARY KEY NOT ENFORCED," +
                        " name STRING" +
                        ") WITH (" +
                        " 'connector' = 'jdbc'," +
                        " 'url' = 'jdbc:mysql://localhost:3306/flink_db'," +
                        " 'table-name' = 'student_scores'," +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver'," +
                        " 'username' = 'root'," +
                        " 'password' = '123456'" +
                        ")"
        );
        tr.print();
        // ✅ 3. Insert Static Data into Flink Table - source data - raw data
        List<Row> studentData = Arrays.asList(
                Row.of(1, "Alice"),
                Row.of(2, "Bob"),
                Row.of(3, "Charlie"));

        Table studentTable = tableEnv.fromValues(studentData);      //reading the data from list dataset
        tableEnv.createTemporaryView("students_view", studentTable);        //transformation
        // ✅ 4. Insert Data into MySQL  as my SINK.
        tableEnv.executeSql("INSERT INTO student_scores SELECT * FROM students_view");
        tableEnv.executeSql("SELECT * FROM student_scores").print();
    }

}
