package com.redhat.demo.clnr;

import java.util.Collections;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * Builds the data processing pipeline for meter readings
 *
 * @author hhiden
 */
public class ProcessingPipe {
    private String inputStreamName;
    
    public ProcessingPipe(String inputStreamName) {
        this.inputStreamName = inputStreamName;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(inputStreamName, Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String value) {
                        return new MeterReading(value).customerId;
                    }
                })
                .flatMapValues(new ValueMapperWithKey<String, String, Iterable<MeterReading>>() {
                    @Override
                    public Iterable<MeterReading> apply(String readOnlyKey, String value) {
                        return Collections.singletonList(new MeterReading(value));
                    }
                })
                .groupByKey(Serialized.with(new Serdes.StringSerde(), new MeterReadingSerde()))
                .aggregate(new Initializer<CustomerRecord>() {
                    @Override
                    public CustomerRecord apply() {
                        return new CustomerRecord();
                    }
                }, new Aggregator<String, MeterReading, CustomerRecord>() {
                    @Override
                    public CustomerRecord apply(String key, MeterReading value, CustomerRecord aggregate) {
                        return aggregate;
                    }
                }, new CustomerRecordSerde())
                .toStream()
                .foreach(new ForeachAction<String, CustomerRecord>() {
                    @Override
                    public void apply(String key, CustomerRecord value) {
                        System.out.println(key + "=" + value);
                    }
                });

        return builder.build();
    }
}
