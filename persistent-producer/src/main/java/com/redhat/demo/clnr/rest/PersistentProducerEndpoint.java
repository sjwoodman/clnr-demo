package com.redhat.demo.clnr.rest;

import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.JsonObject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import com.redhat.demo.clnr.model.Reading;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;

import java.util.logging.Logger;

@Stateless
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
@Path("/clnr")
public class PersistentProducerEndpoint {

    private final static Logger logger = Logger.getLogger(PersistentProducerEndpoint.class.getName());

    @Producer
    private SimpleKafkaProducer<String, JsonObject> myproducer;

    //Configured in pom.xml for OCP deployments or local environment variables
    private static final String PRODUCER_OUTPUT_TOPIC = System.getenv("PRODUCER_OUTPUT_TOPIC");


    @PersistenceContext(unitName = "reading-persistence-unit")
    private EntityManager em;

    @POST
    @Path("/reading")
    @Consumes("application/json")
    public Response createReading(Reading r) {
        logger.fine(r.toString());

        persistReading(r);

        return Response.created(
                UriBuilder.fromResource(PersistentProducerEndpoint.class)
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
            persistReading(r);

            logger.fine(r.toString());

            return Response.created(
                    UriBuilder.fromResource(PersistentProducerEndpoint.class)
                            .path(String.valueOf(r.getId())).build()).build();
        }
    }

    @GET
    @Path("/reading/{id}")
    @Produces("application/json")
    public Reading getReading(@PathParam("id") Long id) {

        return em.find(Reading.class, id);
    }

    /**
     * Persist a reading to the DB and send a kafka message
     * @param r Reading to be stored
     */
    private void persistReading(Reading r){
        em.persist(r);

        //Send a JSON object with the timestamp and kWh values
        JsonObject reading = Json.createObjectBuilder()
                .add("date", r.getTimestamp())
                .add("kWh", r.getkWh())
                .build();
        myproducer.send(PRODUCER_OUTPUT_TOPIC, r.getCustomerId(), reading);

    }
}
