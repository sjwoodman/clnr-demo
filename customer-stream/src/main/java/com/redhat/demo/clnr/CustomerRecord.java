package com.redhat.demo.clnr;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds summary data for a specific customer
 * @author hhiden
 */
public class CustomerRecord implements Serializable {
    public String customerId;
    private Map<Integer, Double> hourBins;

    public CustomerRecord() {
        initHourBins();
    }

    public CustomerRecord(String customerId) {
        this.customerId = customerId;
        initHourBins();
    }
    
    private void initHourBins(){
        hourBins = new HashMap<>();
        for(int i=0;i<23;i++){
            hourBins.put(i, 0.0);
        }
    }
    
}
