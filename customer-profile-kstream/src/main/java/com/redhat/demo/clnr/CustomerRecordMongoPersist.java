package com.redhat.demo.clnr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.bson.Document;

/**
 * Attach to a KStream and persist data into MongoDB
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class CustomerRecordMongoPersist {
    private static final Logger logger = Logger.getLogger(CustomerRecordMongoPersist.class.getName());
    
    ObjectMapper mapper = new ObjectMapper();
    MongoClient client;
    MongoDatabase db;
    MongoCollection<Document> profilesCollection;

    public CustomerRecordMongoPersist() {
        logger.info("Connecting to Mongo");
        MongoCredential credential = MongoCredential.createCredential("mongo", "profiles", "mongo".toCharArray());
        
        client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> 
                        builder.hosts(Arrays.asList(new ServerAddress("mongodb", 27017)))).credential(credential).build());
        db = client.getDatabase("profiles");
        profilesCollection = db.getCollection("customers");
        logger.info("Mongo connected");
    }
    
    /** Respond to customer profile updates */
    @Consumer(topics = "profile.out", groupId = "1")
    public void onMessage(String key, String value){
        try {
            Document doc = Document.parse(value);
            String customerId = doc.getString("customerId");
            Document query = new Document("customerId", customerId);
            Iterator<Document> docs = profilesCollection.find(query).iterator();
            if(docs.hasNext()){
                // Already one 
                profilesCollection.findOneAndReplace(query, doc);
            } else {
                // Add
                profilesCollection.insertOne(doc);
            }
            
            /*
            Document updated = profilesCollection.findOneAndUpdate(new Document("customerId", customerId), doc);
            if(updated==null){
                logger.info("Added customer record: " + doc.getString("customerId"));
                profilesCollection.insertOne(doc);
            }
            */
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
        
    }
    
    @PostConstruct
    public void init(){
        System.out.println("CustomerRecordMongoPersist.init");
    }
    
    @PreDestroy
    public void disconnect(){
        logger.info("DBClose");
    }
   
}