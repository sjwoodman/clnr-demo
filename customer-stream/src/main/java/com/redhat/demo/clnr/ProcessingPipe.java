package com.redhat.demo.clnr;

import com.redhat.demo.clnr.operations.CSVKeyExtractor;
import com.redhat.demo.clnr.operations.CSVTimestampExtractor;
import com.redhat.demo.clnr.operations.MeterReadingParser;
import com.redhat.demo.clnr.operations.MeterReadingReducer;
import com.redhat.demo.clnr.operations.MeterReadingTimstampExtractor;
import java.awt.Window;
import java.text.SimpleDateFormat;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
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

    private String inputStreamName;

    public ProcessingPipe(String inputStreamName) {
        this.inputStreamName = inputStreamName;
    }

    public Topology getTopology() {

        final StreamsBuilder builder = new StreamsBuilder();
        CSVTimestampExtractor extractor = new CSVTimestampExtractor(3, new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"));
        builder.<String, String>stream(inputStreamName, Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(extractor))
                .selectKey(new CSVKeyExtractor(0))
                .flatMapValues(new MeterReadingParser())
                .groupByKey(Serialized.with(new Serdes.StringSerde(), new MeterReadingSerde()))
                .windowedBy(TimeWindows.of(24 * 60 * 60 * 1000))
                //.aggregate(()->0.0, (k,v,a)->a + v.value, Materialized.<String,Double,WindowStore<Bytes, byte[]>>as("sum-store").withValueSerde(Serdes.Double()).withKeySerde(Serdes.String()))
                .aggregate(()->new CustomerRecord(), (k,v,a)->a.update(v), Materialized.<String,CustomerRecord,WindowStore<Bytes, byte[]>>as("sum-store").withValueSerde(new CustomerRecordSerde()).withKeySerde(Serdes.String()))
                .toStream()
                .foreach(new ForeachAction<Object, Object>() {
                    @Override
                    public void apply(Object key, Object value) {
                        System.out.println(key + ":" + value);
                    }
                });
        return builder.build();
    }
}
