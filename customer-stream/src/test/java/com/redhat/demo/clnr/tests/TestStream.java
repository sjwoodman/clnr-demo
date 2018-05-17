/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redhat.demo.clnr.tests;

import com.redhat.demo.clnr.CustomerRecord;
import com.redhat.demo.clnr.CustomerRecordSerde;
import com.redhat.demo.clnr.MeterReading;
import com.redhat.demo.clnr.MeterReadingSerde;
import com.redhat.demo.clnr.ProcessingPipe;
import com.redhat.demo.clnr.operations.CSVKeyExtractor;
import com.redhat.demo.clnr.operations.CSVTimestampExtractor;
import com.redhat.demo.clnr.operations.MeterReadingParser;
import com.redhat.demo.clnr.operations.MeterReadingTimstampExtractor;
import io.debezium.kafka.KafkaCluster;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Locally embedded Kafka test
 *
 * @author hhiden
 */
public class TestStream {

    private static final Logger logger = Logger.getLogger(TestStream.class.getName());

    protected static KafkaCluster cluster;
    protected static File dataDir;
    protected static DataProducer producerThread;

    protected static KafkaCluster kafkaCluster() {
        if (cluster != null) {
            throw new IllegalStateException();
        }
        dataDir = new File("/tmp/MeterReadings");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        cluster = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .withPorts(2181, 9092);
        return cluster;
    }

    public TestStream() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        cluster = kafkaCluster().addBrokers(1).startup();
        producerThread = new DataProducer("stream-meter-readings-input");
    }

    @AfterClass
    public static void tearDownClass() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
            if (!dataDir.delete()) {
                dataDir.deleteOnExit();
            }
        }
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        // Shut down the pipeline...

    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
    @Test
    public void processData() throws Exception {
        // Create a producer for meter readings

        // Join the pipe to this stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "meter-readings");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        
        final StreamsBuilder builder = new StreamsBuilder();
        CSVTimestampExtractor extractor = new CSVTimestampExtractor(3, new SimpleDateFormat("mm/dd/yyyy HH:mm:ss"));
        builder.<String, String>stream("stream-meter-readings-input", Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(extractor))
                .selectKey(new CSVKeyExtractor(0))
                .flatMapValues(new MeterReadingParser())
                .groupByKey(Serialized.with(new Serdes.StringSerde(), new MeterReadingSerde()))
                .aggregate(new Initializer<CustomerRecord>() {
                    @Override
                    public CustomerRecord apply() {
                        return new CustomerRecord();
                    }
                }, new Aggregator<String, MeterReading, CustomerRecord>() {
                    @Override
                    public CustomerRecord apply(String key, MeterReading value, CustomerRecord aggregate) {
                        return aggregate;
                    }
                }, new CustomerRecordSerde())
                .toStream()
                .foreach(new ForeachAction<String, CustomerRecord>() {
                    @Override
                    public void apply(String key, CustomerRecord value) {
                        System.out.println(key + "=" + value);
                    }
                });


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        //ProcessingPipe pipe = new ProcessingPipe("stream-meter-readings-input");
        //KafkaStreams streams = new KafkaStreams(pipe.getTopology(), props);
        streams.start();
        producerThread.start();

        Thread.sleep(5000); //Noooo
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "meter-readings-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static class DataProducer extends Thread {
        private String topic;

        public DataProducer(String topic) {
            this.topic = topic;
            setDaemon(true);
        }

        @Override
        public void run() {
            logger.info("Starting DataProducer Thread");
            URL url = TestStream.class.getResource("/data.csv");
            File dataFile = new File(url.getFile());
            logger.info(dataFile.getPath());
            
            KafkaProducer<String, String> producer = createProducer();
            try (FileReader fileReader = new FileReader(dataFile)) {
                try (BufferedReader reader = new BufferedReader(fileReader)) {
                    String row;
                    while ((row = reader.readLine()) != null) {
                        // Produce a message
                        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, "", row);

                        producer.send(record);
                    }
                }
            } catch (IOException ioe) {
                fail(ioe.getMessage());
            }
            logger.info("All data sent");
            producer.flush();
            producer.close();
            logger.info("Producer closed");
        }
    }
}
