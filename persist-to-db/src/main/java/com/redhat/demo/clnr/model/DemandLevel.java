package com.redhat.demo.clnr.model;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * This class represents the total demand aggregated over all of the consumers for a time period.
 *
 * Annotated for pushing into DB.
 *
 * @author hhiden
 */
@Entity
@XmlRootElement
public class DemandLevel implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    private Long id;

    @Column
    public Date timestamp;

    @Column
    public double demand;

    public DemandLevel() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public double getDemand() {
        return demand;
    }

    public void setDemand(double demand) {
        this.demand = demand;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DemandLevel)) return false;
        DemandLevel that = (DemandLevel) o;
        return Double.compare(that.demand, demand) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, timestamp, demand);
    }

    @Override
    public String toString() {
        return "DemandLevel{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", demand=" + demand +
                '}';
    }
}