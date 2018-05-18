/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.demo.clnr.operations;

import com.redhat.demo.clnr.MeterReading;
import java.text.DateFormat;
import org.apache.kafka.streams.kstream.Reducer;

/**
 *
 * @author hhiden
 */
public class MeterReadingReducer implements Reducer<MeterReading>{
    private static final DateFormat fmt = DateFormat.getDateTimeInstance();

    public MeterReadingReducer() {
        System.out.println("Created meter reducer");
    }

    @Override
    public MeterReading apply(MeterReading value1, MeterReading value2) {
        //System.out.println(value1 + "->" + value2);
        return value2;
    }
    
}
