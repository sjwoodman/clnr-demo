package com.redhat.demo.clnr;

import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.logging.Logger;

@ApplicationScoped
@Path("/clnr")
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class IngestAPI {

    private final static Logger logger = Logger.getLogger(IngestAPI.class.getName());

    @Producer
    private SimpleKafkaProducer<String, Reading> myproducer;

    private static final String OUTPUT_TOPIC = System.getenv("INGEST_API_OUT");

    @GET
    @Produces("text/plain")
    public String test() {
        logger.info("test");
        return "x";
    }

    @POST
    @Path("/reading")
    @Consumes("application/json")
    public Response createReading(Reading r) {
        logger.fine(r.toString());

        sendReading(r);

        return Response.created(
                UriBuilder.fromResource(IngestAPI.class)
                        .path(String.valueOf(r.getId())).build()).build();
    }

    @POST
    @Path("/reading/csv")
    @Consumes("text/plain")
    public Response createReading(String csv) {

        logger.info(csv);

        String[] parts = csv.split(",");

        if (parts.length != 5) {
            logger.warning("Unexpected line length for: " + csv);
            return Response.status(Response.Status.BAD_REQUEST).build();
        } else {

            Reading r = new Reading(parts[0], parts[3], Double.valueOf(parts[4]));
            sendReading(r);

            logger.fine(r.toString());

            return Response.created(
                    UriBuilder.fromResource(IngestAPI.class)
                            .path(String.valueOf(r.getId())).build()).build();
        }

    }

    private void sendReading(Reading r) {

        myproducer.send(OUTPUT_TOPIC, r.getCustomerId(), r);

    }
}
