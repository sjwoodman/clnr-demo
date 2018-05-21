package com.redhat.demo.clnr;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds summary data for a specific customer
 * @author hhiden
 */
public class CustomerRecord implements Serializable {
    public static final long serialVersionUID = 0L;
    public String customerId;
    public HashMap<Integer, Double> hourBins = new HashMap<>();

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
            hourBins.put(hour, existing + reading.value);
            return this;
        } catch (Exception e){
            e.printStackTrace();
            return this;
        }
    }
    
    private void initHourBins(){
        for(int i=0;i<24;i++){
            hourBins.put(i, 0.0);
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
    
    
}
