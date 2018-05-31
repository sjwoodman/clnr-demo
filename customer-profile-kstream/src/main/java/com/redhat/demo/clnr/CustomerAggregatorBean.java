package com.redhat.demo.clnr;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.util.Date;
import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.stream.JsonParser;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

/**
 * This class attaches to a KStream of MeterReadings and aggregates the data into customer profiles
 *
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class CustomerAggregatorBean {

    private final ObjectMapper mapper = new ObjectMapper();

    @KafkaStream(input = "ingest.api.out", output = "profile.out")
    public KStream<String, String> demandStream(final KStream<String, JsonObject> source) {

        return source
                .selectKey((key, value) -> value.getString("customerId"))
                .map((key, value) -> {
                    MeterReading mr = new MeterReading();
                    mr.setCustomerId(value.getString("customerId"));
                    mr.setTimestamp(value.getString("timestamp"));
                    mr.setValue(value.getJsonNumber("kWh").doubleValue());
                    return new KeyValue<>(key, mr);
                })
                
                .groupByKey(Serialized.with(new Serdes.StringSerde(), new MeterReadingSerializer()))
                /*.windowedBy(TimeWindows.of(24 * 60 * 60 * 1000).until(96 * 60 * 60 * 1000))*/
                .aggregate(()->new CustomerRecord(), (k, v, a)-> a.update(v), CafdiSerdes.Generic(CustomerRecord.class))
                
                /*
                .aggregate(() -> new CustomerRecord(), (k, v, a) -> a.update(v),
                        Materialized.<String, CustomerRecord, WindowStore<Bytes, byte[]>>as("sum-store")
                                .withValueSerde(CafdiSerdes.Generic(CustomerRecord.class))
                                .withKeySerde(Serdes.String()))
                */
                .toStream().map((k, v)->{
                    String json = "";
                    try {
                        json = mapper.writeValueAsString(v);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    return new KeyValue<>(v.customerId, json);                    
                });


    }
}
