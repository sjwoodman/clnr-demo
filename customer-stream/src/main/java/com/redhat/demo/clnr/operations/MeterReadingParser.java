package com.redhat.demo.clnr.operations;

import com.redhat.demo.clnr.MeterReading;
import java.util.Collections;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * Parse meter readings
 *
 * @author hhiden
 */
public class MeterReadingParser implements ValueMapperWithKey<String, String, Iterable<MeterReading>> {

    @Override
    public Iterable<MeterReading> apply(String readOnlyKey, String value) {
        return Collections.singletonList(new MeterReading(value));
    }

}
