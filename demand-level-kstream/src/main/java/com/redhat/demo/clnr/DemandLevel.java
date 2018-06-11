package com.redhat.demo.clnr;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class represents the total demand aggregated over all of the consumers for a time period
 * @author hhiden
 */
public class DemandLevel implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public String timestamp;
    public double demand;

    public DemandLevel() {
    }

    public DemandLevel(String timestamp, double demand) {
        this.timestamp = timestamp;
        this.demand = demand;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setDemand(double demand) {
        this.demand = demand;
    }

    public double getDemand() {
        return demand;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return timestamp + " --" + demand + " KWh";
    }
}