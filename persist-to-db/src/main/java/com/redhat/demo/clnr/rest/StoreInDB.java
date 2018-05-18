package com.redhat.demo.clnr.rest;

import javax.ejb.Stateless;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.redhat.demo.clnr.model.Reading;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

import java.util.logging.Logger;

@Stateless
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class StoreInDB {

    private final static Logger logger = Logger.getLogger(StoreInDB.class.getName());

    @PersistenceContext(unitName = "MyPU")
    private EntityManager em;

    @Consumer(topics = "#{PERSIST_DB_IN}", groupId = "1")
    public void receiver(final String key, final Reading r) {

        em.persist(r);

        logger.info("Id: " + key + ", Timestamp: " + r.getTimestamp() + ", kWh: " + r.getkWh());
    }
}
