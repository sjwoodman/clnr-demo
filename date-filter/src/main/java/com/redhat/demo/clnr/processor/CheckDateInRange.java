package com.redhat.demo.clnr.processor;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.annotations.SourceType;

/**
 * Make sure that a reading is within the specified date range
 * @author hhiden
 */
@MessageProcessor(configSource = SourceType.ENVIRONMENT, serverName = "KAFKA_SERVICE_HOST", port = "KAFKA_SERVICE_PORT")
public class CheckDateInRange {
    @HandleMessage(
            configSource = SourceType.SPECIFIED, 
            inputName = "processor.in", 
            outputName = "processor.out", 
            outputType = OutputType.TOPIC)
    public Object checkDate(Object input){
        return input;
    }
}