package com.redhat.demo.clnr;

import java.io.Serializable;
import java.util.Date;

/**
 * This class represents the total demand aggregated over all of the consumers for a time period
 * @author hhiden
 */
public class DemandLevel implements Serializable {
    public Date timestamp;
    public double demand;
}