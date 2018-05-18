package com.redhat.demo.clnr;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Contains a single row from the readings file parsed into sections
 *
 * @author hhiden
 */
public class MeterReading implements Serializable {
    private static final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    
    public String customerId;
    public Date timestamp;
    public double value;

    public MeterReading(String row) {
        try {
            String[] parts = row.split(",");
            customerId = parts[0];
            timestamp = format.parse(parts[3]);
            value = Double.parseDouble(parts[4]);
        } catch (Exception e) {
            System.out.println("Error parsing data: " + e.getMessage());
            customerId = "000";
            value = 0;
            timestamp = new Date(0L);
        }
    }

    @Override
    public String toString() {
        return customerId + ":" + format.format(timestamp) + "=" + value;
    }
}
