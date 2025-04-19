//package com.niit.ch3.connectors;
//
//import com.mongodb.client.*;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.bson.Document;
//import java.util.Iterator;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//
//class MongoDBSource implements SourceFunction<Document> {
//    private final String mongoUri;
//    private final String databaseName;
//    private final String collectionName;
//    private volatile boolean running = true;
//
//    public MongoDBSource(String mongoUri, String databaseName, String collectionName) {
//        this.mongoUri = mongoUri;
//        this.databaseName = databaseName;
//        this.collectionName = collectionName;
//    }
//
//    @Override
//    public void run(SourceContext<Document> ctx) {
//        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
//            MongoDatabase database = mongoClient.getDatabase(databaseName);
//            MongoCollection<Document> collection = database.getCollection(collectionName);
//
//            while (running) {
//                FindIterable<Document> documents = collection.find();
//                Iterator<Document> iterator = documents.iterator();
//
//                while (iterator.hasNext()) {
//                    ctx.collect(iterator.next());
//                }
//                Thread.sleep(5000); // Delay to avoid excessive polling
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void cancel() {
//        running = false;
//    }
//}
//class MongoDBSink implements SinkFunction<String> {
//    private final String mongoUri;
//    private final String databaseName;
//    private final String collectionName;
//
//    public MongoDBSink(String mongoUri, String databaseName, String collectionName) {
//        this.mongoUri = mongoUri;
//        this.databaseName = databaseName;
//        this.collectionName = collectionName;
//    }
//
//    @Override
//    public void invoke(String value, Context context) {
//        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
//            MongoDatabase database = mongoClient.getDatabase(databaseName);
//            MongoCollection<Document> collection = database.getCollection(collectionName);
//
//            Document document = Document.parse(value);
//            document.remove("_id"); // Remove the existing _id to avoid duplication
//
//            collection.insertOne(document);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
//public class FlinkMongoDBJob {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        String mongoUri = "mongodb://localhost:27017";
//        String database = "flink_db";
//        String sourceCollection = "input_collection";
//        String sinkCollection = "output_collection";
//
//        // Source: Read from MongoDB
//        DataStream<Document> sourceStream = env.addSource(new MongoDBSource(mongoUri, database, sourceCollection));
//
//        // Transformation: Convert Document to String (for simplicity)
//        DataStream<String> processedStream = sourceStream.map((MapFunction<Document, String>) Document::toJson);
//
//        // Sink: Write to MongoDB
//        processedStream.addSink(new MongoDBSink(mongoUri, database, sinkCollection));
//
//        env.execute("Flink MongoDB Connector Example");
//    }
//}
