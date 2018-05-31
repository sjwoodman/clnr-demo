package com.redhat.demo.clnr;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.util.logging.Logger;
import javax.ejb.Stateless;
import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.stream.JsonParser;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

/**
 * Attach to a KStream and persist data into MongoDB
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class CustomerRecordMongoPersist {
    private static final Logger logger = Logger.getLogger(CustomerRecordMongoPersist.class.getName());
    
    ObjectMapper mapper = new ObjectMapper();
    

    public CustomerRecordMongoPersist() {
    }
    
    /** Respond to customer profile updates */
    @Consumer(topics = "profile.out", groupId = "1")
    public void onMessage(String key, String value){
        try {
            CustomerRecord record = mapper.readValue(value, CustomerRecord.class);
            logger.info(record.toString());
            
        } catch(Exception e){
            e.printStackTrace();
        }
        
    }
}