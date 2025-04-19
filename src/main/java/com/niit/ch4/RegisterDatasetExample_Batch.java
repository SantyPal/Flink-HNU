package com.niit.ch4;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.*;
public class RegisterDatasetExample_Batch {
    public static void main(String[] args) {
        // Use TableEnvironment with batch mode
        //This initializes Flink’s TableEnvironment in batch mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()        //flink setting for table
        //constructs the environment settings for batch processing.
                .inBatchMode()           //batch mode data (bounded data, limited data)
                .build();           //finish building the table envuironment
        // Create TableEnvironment using the defined settings
        //The TableEnvironment allows execution of SQL queries and management of tables.
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // Create a temporary table with a PRINT sink (for debugging)
        tableEnv.executeSql(                            //transformation
                "CREATE TEMPORARY TABLE people (" +
                        "  name STRING, " +
                        "  age INT" +
                        ") WITH (" +
                        "  'connector' = 'print' " + // The results will be printed to the console instead of being stored in a real database or file system.
                        ")"
        );
        // inserts three records into the people table (will print instead of storing)
        tableEnv.executeSql("INSERT INTO people VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 35)");       //dataset

        // Executes a SQL SELECT query on the registered people table.
        //The results are stored in a Table object
        Table result = tableEnv.sqlQuery("SELECT * FROM people ");
       //execute() triggers the query execution.
        //print() prints the result set to the console.
        result.execute().print();                   //sink
        //+I (Insert) indicates that these records are being inserted.
    }
}
