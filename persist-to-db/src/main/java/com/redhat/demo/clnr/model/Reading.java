package com.redhat.demo.clnr.model;

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Objects;

@Entity
@XmlRootElement
public class Reading implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    private Long id;

    @Column
    private String customerId;

    @Column
    private String timestamp;

    @Column
    private Double kWh;

    public Reading() {
    }

    public Reading(String customerId, String timestamp, Double kWh) {
        this.customerId = customerId;
        this.timestamp = timestamp;
        this.kWh = kWh;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getkWh() {
        return kWh;
    }

    public void setkWh(Double kWh) {
        this.kWh = kWh;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Reading)) return false;
        Reading reading = (Reading) o;
        return Objects.equals(id, reading.id) &&
                Objects.equals(customerId, reading.customerId) &&
                Objects.equals(timestamp, reading.timestamp) &&
                Objects.equals(kWh, reading.kWh);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, customerId, timestamp, kWh);
    }

    @Override
    public String toString() {
        return "Reading{" +
                "id=" + id +
                ", customerId='" + customerId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", kWh=" + kWh +
                '}';
    }
}
