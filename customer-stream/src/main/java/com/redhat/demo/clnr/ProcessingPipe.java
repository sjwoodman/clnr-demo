package com.redhat.demo.clnr;

import com.redhat.demo.clnr.operations.MeterReadingTimstampExtractor;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Builds the data processing pipeline for meter readings
 *
 * @author hhiden
 */
public class ProcessingPipe {

    private KStream outStream;
    private KStream demandStream;
    private String inputStreamName;

    public ProcessingPipe(String inputStreamName) {
        this.inputStreamName = inputStreamName;
    }

    public Topology build() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream s1 = builder.<String, MeterReading>stream(inputStreamName,
                Consumed.with(Serdes.String(), CafdiSerdes.Generic(MeterReading.class))
                        .withTimestampExtractor(new MeterReadingTimstampExtractor()));

        outStream = attachCustomerRecordStream(s1);
        demandStream = attachGlobalDemandLevelStream(s1);
        return builder.build();
    }

    public KStream getDemandStream() {
        return demandStream;
    }

    public KStream getOutStream() {
        return outStream;
    }

    public KStream<String, DemandLevel> attachGlobalDemandLevelStream(KStream<String, MeterReading> source) {
                source
                        .selectKey((key, value) -> {
                    return "ALL";
                })
                .groupByKey(Serialized.with(new Serdes.StringSerde(), CafdiSerdes.Generic(MeterReading.class)))
                .windowedBy(TimeWindows.of(1 * 60 * 60 * 1000).until(1 * 60 * 60 * 1000))
                .aggregate(()->0.0, (k,v,a)->a + v.value,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("demand-store")
                                .withValueSerde(Serdes.Double())
                                .withKeySerde(Serdes.String()))                        
                .toStream().foreach(new ForeachAction<Windowed<String>, Double>() {
                    @Override
                    public void apply(Windowed<String> key, Double value) {
                        System.out.println(SimpleDateFormat.getDateTimeInstance().format(new Date(key.window().start())) + ":" + value);
                    }
        });
                
        return null;
    }

    public KStream<String, CustomerRecord> attachCustomerRecordStream(KStream<String, MeterReading> source) {
        return source
                .selectKey((key, value) -> value.customerId)
                .groupByKey(Serialized.with(new Serdes.StringSerde(), CafdiSerdes.Generic(MeterReading.class)))
                .windowedBy(TimeWindows.of(24 * 60 * 60 * 1000).until(96 * 60 * 60 * 1000))
                .aggregate(() -> new CustomerRecord(), (k, v, a) -> a.update(v),
                        Materialized.<String, CustomerRecord, WindowStore<Bytes, byte[]>>as("sum-store")
                                .withValueSerde(CafdiSerdes.Generic(CustomerRecord.class))
                                .withKeySerde(Serdes.String()))
                .toStream()
                .map((key, value) -> {
                    value.setWindowStart(new Date(key.window().start()));
                    return new KeyValue<>(value.customerId, value);
                });

    }
}
