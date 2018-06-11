package com.redhat.demo.clnr;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Holds summary data for a specific customer
 * @author hhiden
 */
public class CustomerRecord implements Serializable {
    public static final long serialVersionUID = 0L;
    public String customerId;
    public HashMap<Integer, Double> hourBins = new HashMap<>();
    public HashMap<Integer, Integer> updateCount = new HashMap<>();
    public Date windowStart;

    public CustomerRecord() {
        initHourBins();
    }

    public CustomerRecord(String customerId) {
        this.customerId = customerId;
        initHourBins();
    }
    
    public CustomerRecord update(MeterReading reading){
        try {
            if(customerId==null){
                customerId = reading.customerId;
            }
            int hour = reading.getHourOfDay();
            double existing = hourBins.get(hour);
            int count = updateCount.get(hour);
            count++;
            double newReading = (existing + reading.value) / (count );
            updateCount.put(hour, count);
            
            hourBins.put(hour, newReading);
            return this;
        } catch (Exception e){
            System.out.println(e.getMessage());
            return this;
        }
    }
    
    private void initHourBins(){
        for(int i=0;i<24;i++){
            hourBins.put(i, 0.0);
            updateCount.put(i, 0);
        }
    }

    @Override
    public String toString() {
        NumberFormat fmt = NumberFormat.getNumberInstance();
        fmt.setMinimumIntegerDigits(1);
        fmt.setMinimumFractionDigits(4);
        fmt.setMaximumFractionDigits(4);
        StringBuilder builder = new StringBuilder();
        builder.append(customerId);
        builder.append(":");
        for(int i=0;i<hourBins.size();i++){
            if(i>0){
                builder.append(",");
            }
            builder.append(fmt.format(hourBins.get(i)));
        }
        return builder.toString();
    }

    public Date getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Date windowStart) {
        this.windowStart = windowStart;
    }
}