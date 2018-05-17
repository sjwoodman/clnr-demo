package com.redhat.demo.clnr.operations;

import com.redhat.demo.clnr.MeterReading;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Extracts timestamps from a meter reading stream
 * @author hhiden
 */
public class MeterReadingTimstampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return ((MeterReading)record.value()).timestamp.getTime();
    }
    
}
