package com.redhat.demo.clnr;

import com.redhat.demo.clnr.cloudevents.KafkaHeaderUtil;
import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

@ApplicationScoped
@Path("/clnr")
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class IngestAPI {
    private static final DateTimeFormatter format = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
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
    @Path("/ce")
    @Consumes("application/json")
    public Response createReading(CloudEventImpl ce) {

        Reading r = new Reading();

        //headers
        Iterable<Header> headers = KafkaHeaderUtil.getHeaders(ce);

        if (ce.getData().isPresent()) {

            Map data = (Map) ce.getData().get();
            if (data.containsKey("customerId")) {
                r.setCustomerId((String) data.get("customerId"));
            }

            if (data.containsKey("kWh")) {
                r.setkWh((Double) data.get("kWh"));
            }

            if(ce.getEventTime().isPresent())
            {
                r.setTimestamp(ce.getEventTime().get().toString());
            }

            logger.fine(r.toString());

            sendReading(headers, r);

            return Response.created(
                    UriBuilder.fromResource(IngestAPI.class)
                            .path(String.valueOf(r.getId())).build()).build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
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
        sendReading(new ArrayList<>(), r);
    }

    private void sendReading(Iterable<Header> headers, Reading r) {
        ZonedDateTime zd = ZonedDateTime.parse(r.getTimestamp(), format);

        long timestamp = zd.toInstant().toEpochMilli();

        ProducerRecord<String, Reading> record = new ProducerRecord<>(OUTPUT_TOPIC, null, timestamp, r.getCustomerId(), r, headers);
        ((org.apache.kafka.clients.producer.Producer) myproducer).send(record);

        //myproducer.send(OUTPUT_TOPIC, r.getCustomerId(), r);
    }
}
