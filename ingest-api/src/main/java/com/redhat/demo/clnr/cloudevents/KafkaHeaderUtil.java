package com.redhat.demo.clnr.cloudevents;

import io.streamzi.cloudevents.CloudEvent;
import io.streamzi.cloudevents.impl.CloudEventImpl;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.ArrayList;
import java.util.List;

public class KafkaHeaderUtil {

    public static final String EVENT_TYPE = "EventType";
    public static final String CLOUDEVENTS_VERSION = "CloudEventsVersion";
    public static final String SOURCE = "Source";
    public static final String EVENT_ID = "EventID";
    public static final String EVENT_TYPE_VERSION = "EventTypeVersion";
    public static final String SCHEMA_URL = "SchemaURL";
    public static final String CONTENT_TYPE = "ContentType";

    public static Iterable<Header> getHeaders(CloudEventImpl ce) {

        List<Header> headers = new ArrayList<>();

        headers.add(new RecordHeader(EVENT_TYPE, ce.getEventType().getBytes()));
        headers.add(new RecordHeader(CLOUDEVENTS_VERSION, ce.getCloudEventsVersion().getBytes()));
        headers.add(new RecordHeader(SOURCE, ce.getSource().toString().getBytes()));
        headers.add(new RecordHeader(EVENT_ID, ce.getEventID().getBytes()));

        if (ce.getEventTypeVersion().isPresent()) {
            headers.add(new RecordHeader(EVENT_TYPE_VERSION, ((String) ce.getEventTypeVersion().get()).getBytes()));
        }

        if (ce.getSchemaURL().isPresent()) {
            headers.add(new RecordHeader(SCHEMA_URL, (ce.getSchemaURL().get()).toString().getBytes()));
        }

        if (ce.getContentType().isPresent()) {
            headers.add(new RecordHeader(CONTENT_TYPE, ((String) ce.getContentType().get()).getBytes()));
        }

        //todo: extensions?

        return headers;
    }
}
