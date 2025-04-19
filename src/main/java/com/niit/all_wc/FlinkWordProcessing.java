package com.niit.all_wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class FlinkWordProcessing {
    public static void main(String[] args) throws Exception {
       // 创建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建初始数据集
        DataSet<String> text = env.fromElements(
                "hello world",
                "flink stream processing",
                "real-time analytics",
                "batch processing with flink",
                "map filter flatMap");

        // 1. 使用 map 操作将所有单词转换为大写
        DataSet<String> upperCaseText = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                return value.toUpperCase();
            }
        });
        // 2. 使用 filter 操作过滤掉长度小于 5 个字符的单词
        DataSet<String> filteredText = upperCaseText.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return value.length() >= 5;
            }
        });
        // 3. 使用 flatMap 操作将每个字符串拆分成多个单词，并过滤掉重复的单词
        DataSet<String> flatMappedText = filteredText.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                String[] words = value.split("\\s+");
                HashSet<String> wordSet = new HashSet<>();
                for (String word : words) {
                    if (!wordSet.contains(word)) {
                        out.collect(word);
                        wordSet.add(word);
                    }
                }
            }
        });
        // 打印结果
        flatMappedText.print();
    }
}