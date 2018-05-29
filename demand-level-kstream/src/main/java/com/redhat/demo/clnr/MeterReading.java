package com.redhat.demo.clnr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Contains a single row from the readings file parsed into sections
 *
 * @author hhiden
 */
public class MeterReading implements Serializable {
    public static final long serialVersionUID = 0L;
    
    @JsonIgnore
    private static final SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    
    public String customerId;
    public Date timestamp;
    public double value;

    public MeterReading() {
    }

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

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    
    @JsonIgnore
    public void setTimestamp(String timestamp){
        try {
            this.timestamp = format.parse(timestamp);
        } catch (Exception e){
            System.out.println("Timestamp parse error: " + timestamp);
        }
    }

    public void setValue(double value) {
        this.value = value;
    }
    
    
    @JsonIgnore
    public int getHourOfDay(){
        Calendar c = Calendar.getInstance();
        c.setTime(timestamp);
        return c.get(Calendar.HOUR_OF_DAY);
    }
    
    public String formatDate(){
        return format.format(timestamp);
    }

}
