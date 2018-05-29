package com.redhat.demo.clnr;

import java.util.Date;
import java.util.logging.Logger;
import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * CDI managed bean to calculate a stream of demand levels from a meter reading stream.
 *
 * @author hhiden
 */
@ApplicationScoped
@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT}")
public class DemandLevelBean {
    private static final Logger logger = Logger.getLogger(DemandLevelBean.class.getName());
    
    @KafkaStream(input="ingest.api.out", output="demand.out")
    public KStream<String, Long> demandStream(final KStream<String, JsonObject> source) {
        return source
                .peek((arg0, arg1) -> {
                    System.out.println(arg1.toString());
                })
                .selectKey((key, value) -> {
                    return "ALL";
                }).map((key, value) -> {
                   MeterReading mr = new MeterReading();
                   mr.setCustomerId(value.getString("customerId"));
                   mr.setTimestamp(value.getString("timestamp"));
                   mr.setValue(value.getJsonNumber("kWh").doubleValue());
                   return new KeyValue<>(key, mr);
               })
                .groupByKey(Serialized.with(new Serdes.StringSerde(), CafdiSerdes.Generic(MeterReading.class)))
                .windowedBy(TimeWindows.of(1 * 60 * 60 * 1000).until(1 * 60 * 60 * 1000))
                .aggregate(() -> 0.0, (k, v, a) -> a + v.value,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("demand-store")
                                .withValueSerde(Serdes.Double())
                                .withKeySerde(Serdes.String()))
                .toStream().map(new KeyValueMapper<Windowed<String>, Double, KeyValue<String, Long>>() {
                    @Override
                    public KeyValue<String, Long> apply(Windowed<String> key, Double value) {
                        return new KeyValue<>("DEMAND", (long)(value * 100.0));
                    }
                }
                ).peek((k, v)->logger.info(v.toString()));

    }
}
