
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
 *
 * @author hhiden
 */
public class MeterReadingSerializer implements Serde<MeterReading>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MeterReading> serializer() {
        return new Serializer<MeterReading>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, MeterReading data) {
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()){
                    try (ObjectOutputStream stream = new ObjectOutputStream(buffer)){
                        stream.writeObject(data);
                    }
                    return buffer.toByteArray();
                } catch (IOException ioe){
                    ioe.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer<MeterReading> deserializer() {
        return new Deserializer<MeterReading>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public MeterReading deserialize(String topic, byte[] data) {
                try (ByteArrayInputStream buffer = new ByteArrayInputStream(data)){
                    try(ObjectInputStream in = new ObjectInputStream(buffer)){
                        return (MeterReading)in.readObject();
                    }
                } catch (Exception ioe){
                    ioe.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }
    
    
}
