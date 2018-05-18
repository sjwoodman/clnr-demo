package com.redhat.demo.clnr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer / Deserializer for a customer record object
 *
 * @author hhiden
 */
public class CustomerRecordSerde implements Serde<CustomerRecord> {

    @Override
    public void configure(Map<String, ?> map, boolean bln) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<CustomerRecord> serializer() {
        return new Serializer<CustomerRecord>() {
            @Override
            public void configure(Map<String, ?> map, boolean bln) {

            }

            @Override
            public byte[] serialize(String string, CustomerRecord t) {
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    try (ObjectOutputStream outStream = new ObjectOutputStream(buffer)) {
                        outStream.writeObject(t);
                    }
                    return buffer.toByteArray();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    return new byte[0];
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer<CustomerRecord> deserializer() {
        return new Deserializer<CustomerRecord>() {
            @Override
            public void configure(Map<String, ?> map, boolean bln) {

            }

            @Override
            public CustomerRecord deserialize(String string, byte[] bytes) {
                try (ByteArrayInputStream buffer = new ByteArrayInputStream(bytes)){
                    try (ObjectInputStream inStream = new ObjectInputStream(buffer)) {
                        return (CustomerRecord) inStream.readObject();
                    }                    
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }

}
