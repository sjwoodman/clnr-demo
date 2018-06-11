package com.redhat.demo.clnr;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.ProducerRecord;

@ApplicationScoped
@Path("/clnr")
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class IngestAPI {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
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

        String[] parts = csv.split(",");

        if (parts.length != 4) {
            logger.warning("Unexpected line length for: " + csv);
            return Response.status(Response.Status.BAD_REQUEST).build();
        } else {

            String timestamp = (parts[0] + " " + parts[1]).replace(".", "/");
            Reading r = new Reading(parts[2], timestamp, Double.valueOf(parts[3]));
            logger.info(r.toString());
            sendReading(r);

            return Response.created(
                    UriBuilder.fromResource(IngestAPI.class)
                            .path(String.valueOf(r.getId())).build()).build();
        }

    }

    private void sendReading(Reading r) {
        try {
            long timestamp = format.parse(r.getTimestamp()).getTime();
            ProducerRecord<String, Reading> record = new ProducerRecord<>(OUTPUT_TOPIC, null, timestamp, r.getCustomerId(), r);
            ((org.apache.kafka.clients.producer.Producer)myproducer).send(record);
            //myproducer.send(OUTPUT_TOPIC, r.getCustomerId(), r);
        } catch (ParseException pe){
            logger.log(Level.SEVERE, "Error parsing timestamp[" + r.getTimestamp() + "]: " + pe.getMessage());
        }
    }
}
