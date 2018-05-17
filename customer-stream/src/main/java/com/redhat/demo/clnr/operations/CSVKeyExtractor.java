package com.redhat.demo.clnr.operations;

import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * Extracts a key from a CSV delimited row of text
 * @author hhiden
 */
public class CSVKeyExtractor implements KeyValueMapper<String, String, String>{
    int index;

    public CSVKeyExtractor(int index) {
        this.index = index;
    }

    @Override
    public String apply(String key, String value) {
        String[] parts = value.split(",");
        return parts[index];
    }
}
