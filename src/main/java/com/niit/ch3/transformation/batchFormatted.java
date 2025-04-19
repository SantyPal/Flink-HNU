package com.niit.ch3.transformation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.common.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.io.TextOutputFormat;

public class batchFormatted {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample data
        DataSet<String> textData = env.fromElements(
                "Hello, World!",
                "Apache Flink is powerful.",
                "Writing formatted text is easy."
        );
        // Custom formatter
        TextOutputFormat.TextFormatter<String> formatter = new TextOutputFormat.TextFormatter<String>() {
            @Override
            public String format(String value) {
                // Example: Convert to uppercase
                return value.toUpperCase();
            }
        };

        // Output file path
        String outputPath = "src/main/resources/formatted_text.txt";

        // Write DataSet to the output file with custom formatting
        textData.writeAsFormattedText(outputPath, formatter);

        // Execute the program
        env.execute("Formatted Text Example");
    }
}