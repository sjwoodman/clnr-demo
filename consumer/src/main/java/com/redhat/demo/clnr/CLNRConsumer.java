package com.redhat.demo.clnr;

import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;
import javax.ws.rs.Path;
import java.util.logging.Logger;

@ApplicationScoped
@Path("/start")
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class CLNRConsumer {

    private final static Logger logger = Logger.getLogger(CLNRConsumer.class.getName());


    /**
     * Simple listener that receives messages from the Kafka broker
     * <p>
     */
    @Consumer(topics = "#{CONSUMER_INPUT_TOPIC}", groupId = "2")
    public void receiver(final String key, final JsonObject value) {
        logger.info("Id: " + key + ", Timestamp: " + value.getString("timestamp") + ", kWh: " + value.get("kWh"));
    }

}
