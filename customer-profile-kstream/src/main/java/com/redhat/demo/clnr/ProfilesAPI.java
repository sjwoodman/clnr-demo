package com.redhat.demo.clnr;

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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.bson.Document;

/**
 *
 * @author hhiden
 */
@Path("/profiles")
public class ProfilesAPI {
    private static final Logger logger = Logger.getLogger(ProfilesAPI.class.getName());
    
    @GET
    @Path("/customers.csv")
    @Produces("text/csv")
    public Response getProfileData(){
        logger.info("Connecting to Mongo");
        MongoCredential credential = MongoCredential.createCredential("mongo", "profiles", "mongo".toCharArray());
        
        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> 
                        builder.hosts(Arrays.asList(new ServerAddress("mongodb", 27017)))).credential(credential).build());
        MongoDatabase db = client.getDatabase("profiles");
        MongoCollection profilesCollection = db.getCollection("customers");
        
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
        } finally {
            client.close();
        }
    }    
}
