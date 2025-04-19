package com.niit.ch3.connectors;

import com.mongodb.client.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.Document;
import java.util.Iterator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

class MongoDBSource implements SourceFunction<Document> {       //separate class for source
    private final String uri, db, collection;
    public MongoDBSource(String uri, String db, String collection) {    //contructor
        this.uri = uri; this.db = db; this.collection = collection;
    }
    @Override
    public void run(SourceContext<Document> ctx) {
        try (MongoClient client = MongoClients.create(uri)) {
            for (Document doc : client.getDatabase(db).getCollection(collection).find()) {
                ctx.collect(doc);
            }
        }
    }
    @Override public void cancel() {}
}

class MongoDBSink implements SinkFunction<Document> {
    private final String uri, db, collection;
    public MongoDBSink(String uri, String db, String collection) {
        this.uri = uri; this.db = db; this.collection = collection;
    }
    @Override
    public void invoke(Document value, Context context) {
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase(db).getCollection(collection).insertOne(value);
        }
    }
}

public class FlinkMongoDBJob_Total {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String uri = "mongodb://localhost:27017", db = "flink_db";
        env.addSource(new MongoDBSource(uri, db, "input_collection"))
                .addSink(new MongoDBSink(uri, db, "output_collection"));
        env.execute("Flink MongoDB One-Time Insert");
    }
}
