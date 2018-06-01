package com.redhat.demo.clnr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.bson.Document;

/**
 * Attach to a KStream and persist data into MongoDB
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
@Path("/profiles")
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
            System.out.println(query.toJson());
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
    
    public void postConstruct(){
        logger.info("postConstruct");
    }
    
    @PostConstruct
    public void connect(){
        logger.info("DBConnect");
    }
    
    @PreDestroy
    public void disconnect(){
        logger.info("DBClose");
    }
    
    @GET
    @Path("/customers.csv")
    @Produces("text/csv")
    public Response getProfileData(){
        try(ByteArrayOutputStream buffer = new ByteArrayOutputStream()){
            try(PrintWriter writer = new PrintWriter(buffer)){
                // Write a header row
                StringBuilder builder = new StringBuilder();
                builder.append("ID");
                for(int i=0;i<24;i++){
                    builder.append(",");
                    builder.append(i);
                }
                writer.println(builder.toString());
                
                // Get all of the profiles
                Iterator<Document> profiles = profilesCollection.find().iterator();
                Document profile;
                Document bins;
                StringBuilder row;
                
                while(profiles.hasNext()){
                    profile = profiles.next();
                    bins = profile.get("hourBins", Document.class);
                    row = new StringBuilder();
                    row.append(profile.getString("customerId"));
                    
                    for(int i=0;i<24;i++){
                        row.append(",");
                        row.append(bins.get(Integer.toString(i)));
                    }
                    writer.println(row.toString());
                }
                
                
            }
            
            
            // Write the response
            Response r = Response.ok(new ByteArrayInputStream(buffer.toByteArray()), "text/csv").build();
            return r;
        } catch (Exception e){
            Response r = Response.serverError().build();
            return r;
        }
    }
}