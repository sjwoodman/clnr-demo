package com.redhat.demo.clnr;

import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.stream.Stream;

@ApplicationScoped
@Path("/start")
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class ProducerEndpoint {

    private final static Logger logger = Logger.getLogger(ProducerEndpoint.class.getName());

    @Producer
    private SimpleKafkaProducer<String, JsonObject> myproducer;

    //Configured in pom.xml for OCP deployments or local environment variables
    private static final String PRODUCER_OUTPUT_TOPIC = System.getenv("PRODUCER_OUTPUT_TOPIC");

    @GET
    @Produces("text/plain")
    public Response doGet() {

        Stream<String> stream = new BufferedReader(
                new InputStreamReader(
                        getClass().getResourceAsStream("/test.txt"))).lines();

        stream.forEach(item -> {
            //Ignore comments
            if (!item.startsWith("#")) {

                //line format: Location ID,Measurement Description,Parameter Type and Units,Date and Time of capture,Parameter
                String[] parts = item.split(",");
                if (parts.length != 5) {
                    logger.warning("Unexpected line length for: " + item);
                } else {

                    //Send a JSON object with the timestamp and kWh values
                    JsonObject reading = Json.createObjectBuilder()
                            .add("date", parts[3])
                            .add("kWh", parts[4])
                            .build();
                    myproducer.send(PRODUCER_OUTPUT_TOPIC, parts[0], reading);
                }
            }

        });

        return Response.ok("Messages sent").build();

    }
}