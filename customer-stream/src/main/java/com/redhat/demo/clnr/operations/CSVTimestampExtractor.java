/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.demo.clnr.operations;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Extract a timestamp from a CSV row of data
 * @author hhiden
 */
public class CSVTimestampExtractor implements TimestampExtractor {
    private int index;
    private SimpleDateFormat format;

    public CSVTimestampExtractor(int index, SimpleDateFormat format) {
        this.index = index;
        this.format = format;
    }
    
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            String value = record.value().toString();
            String[] parts = value.split(",");
            String timestampText = parts[index];
            Date timestamp = format.parse(timestampText);
            return timestamp.getTime();
        } catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }
}
