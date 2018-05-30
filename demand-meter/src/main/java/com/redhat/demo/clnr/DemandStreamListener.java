package com.redhat.demo.clnr;

import java.util.logging.Logger;
import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

/**
 * Attaches to a Kafka stream to receive demand messages
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class DemandStreamListener {

    public DemandStreamListener() {
        System.out.println("Started demand stream listener");
    }
    
    private static final Logger logger = Logger.getLogger(DemandStreamListener.class.getName());

    @Consumer(topics = "demand.out", groupId = "1")
    public void onMessage(String key, JsonObject json){
        logger.info(json.toString());
        DemandWS.sendDemand(json);
    }
    

}