package com.redhat.demo.clnr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer / Deserializer for a MeterReading
 *
 * @author hhiden
 */
public class MeterReadingSerde implements Serde<MeterReading> {

    @Override
    public void configure(Map<String, ?> map, boolean bln) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MeterReading> serializer() {
        return new Serializer<MeterReading>() {
            @Override
            public void configure(Map<String, ?> map, boolean bln) {

            }

            @Override
            public byte[] serialize(String string, MeterReading t) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (ObjectOutputStream outStream = new ObjectOutputStream(buffer)) {
                    outStream.writeObject(t);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return buffer.toByteArray();
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
            public void configure(Map<String, ?> map, boolean bln) {

            }

            @Override
            public MeterReading deserialize(String string, byte[] bytes) {
                ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
                try (ObjectInputStream inStream = new ObjectInputStream(buffer)) {
                    return (MeterReading) inStream.readObject();
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
