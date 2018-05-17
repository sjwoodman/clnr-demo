package com.redhat.demo.clnr;

import com.redhat.demo.clnr.operations.CSVKeyExtractor;
import com.redhat.demo.clnr.operations.CSVTimestampExtractor;
import com.redhat.demo.clnr.operations.MeterReadingParser;
import com.redhat.demo.clnr.operations.MeterReadingTimstampExtractor;
import java.text.SimpleDateFormat;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;

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
        CSVTimestampExtractor extractor = new CSVTimestampExtractor(3, new SimpleDateFormat("mm/dd/yyyy HH:mm:ss"));
        builder.<String, String>stream(inputStreamName, Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(extractor))
                .selectKey(new CSVKeyExtractor(0))
                .flatMapValues(new MeterReadingParser())
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
