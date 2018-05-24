package com.redhat.demo.clnr;

import java.util.Date;
import java.util.logging.Logger;
import javax.enterprise.context.ApplicationScoped;
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
    public KStream<String, DemandLevel> demandStream(final KStream<String, MeterReading> source) {
        return source
                .selectKey((key, value) -> {
                    return "ALL";
                })
                .groupByKey(Serialized.with(new Serdes.StringSerde(), CafdiSerdes.Generic(MeterReading.class)))
                .windowedBy(TimeWindows.of(1 * 60 * 60 * 1000).until(1 * 60 * 60 * 1000))
                .aggregate(() -> 0.0, (k, v, a) -> a + v.value,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("demand-store")
                                .withValueSerde(Serdes.Double())
                                .withKeySerde(Serdes.String()))
                .toStream().map(new KeyValueMapper<Windowed<String>, Double, KeyValue<String, DemandLevel>>() {
                    @Override
                    public KeyValue<String, DemandLevel> apply(Windowed<String> key, Double value) {
                        return new KeyValue<>("DEMAND", new DemandLevel(new Date(key.window().start()), value));
                    }
                }
                ).peek((k, v)->logger.info(v.toString()));

    }
}
