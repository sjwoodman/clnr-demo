package com.redhat.demo.clnr.rest;

import com.redhat.demo.clnr.model.DemandLevel;
import com.redhat.demo.clnr.model.Reading;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.logging.Logger;

@Stateless
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class StoreInDB {

    private final static Logger logger = Logger.getLogger(StoreInDB.class.getName());

    @PersistenceContext(unitName = "MyPU")
    private EntityManager em;

    @Consumer(topics = "#{PERSIST_DB_IN}", groupId = "1")
    public void receiver(final String key, final Object o) {

        logger.info(o.toString());

        //Explicit casting needed otherwise object deserialised from JSON does not persist
        if (o instanceof Reading) {
            em.persist((Reading) o);
        } else if (o instanceof DemandLevel) {
            em.persist((DemandLevel) o);
        }


    }
}
