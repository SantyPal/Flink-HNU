package com.niit.ch4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegisterDatasetExample_Stream {
    public static void main(String[] args) {
        // Set up the streaming execution environment to process data in real-time.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Define TableEnvironment settings -- This configures the Table API to run in streaming mode instead of batch mode.
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        // Create TableEnvironment
        //This creates a StreamTableEnvironment, which enables interaction between Flink's DataStream API and SQL-based Table API.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Create a DataStream using a POJO class
        //Creates a DataStream containing three Person objects.
        //fromElements() initializes the stream with sample data.
        //Each Person object has two attributes: name (String) and age (int).
        DataStream<Person> stream = env.fromElements(
                new Person("Alice", 25),        //Person class is initialized with name and age.
                new Person("Bob", 30),
                new Person("Charlie", 35)
        );
        // Converts the DataStream<Person> into a Flink Table.
        //Flink infers the schema automatically (i.e., name as STRING, age as INT).
        Table table = tableEnv.fromDataStream(stream);
        // Registers the table as a temporary view named "people".
        //This allows us to write SQL queries on the data.
        tableEnv.createTemporaryView("people", table);
        // Run an SQL query
        Table result = tableEnv.sqlQuery("SELECT name, age FROM people");      //this is working as filter transformation
        // Print results
        result.execute().print();
    }
    // Define a POJO class for the schema
    //A plain old Java object (POJO) is a class definition that is not tied to any Java framework so any Java program can use it.
    //Their primary advantage is their reusability and simplicity.

    public static class Person {
        public String name;
        public int age;
       // Default constructor (required by Flink) for serialization.
        public Person() {}

        public Person(String na, int ag) {
           this.name = na;
            this.age = ag;
        }

    }
}


//new Person("Alice", 25),