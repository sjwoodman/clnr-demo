package com.redhat.demo.clnr.processor;

import com.redhat.processor.annotations.HandleMessage;
import com.redhat.processor.annotations.MessageProcessor;
import com.redhat.processor.annotations.OutputType;
import com.redhat.processor.annotations.ServiceParameter;
import com.redhat.processor.annotations.SourceType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;

/**
 * Make sure that a reading is within the specified date range
 * @author hhiden
 */
@MessageProcessor(configSource = SourceType.ENVIRONMENT, serverName = "KAFKA_SERVICE_HOST", port = "KAFKA_SERVICE_PORT")
public class CheckDateInRange {
    private static final Logger logger = Logger.getLogger(CheckDateInRange.class.getName());
    
    private String format = "mm/dd/yyyy HH:mm:ss";
    
    @ServiceParameter(name="START_DATE")
    private String startDateText;
    
    private SimpleDateFormat sdf;
    private Date startDate;
    
    @HandleMessage(
            configSource = SourceType.SPECIFIED, 
            inputName = "producer.out", 
            outputName = "datefilter.out", 
            outputType = OutputType.TOPIC)
    public JsonObject checkDate(JsonObject input){
        if(input.containsKey("date")){
            String dateText = input.getString("date");
            try {
                Date readingDate = getDateFormat().parse(dateText);
                if(readingDate.after(getStartDate())){
                    logger.info("in range: " + dateText);
                    return input;
                } else {
                    logger.info("Before start date: " + dateText);
                    return null;
                }
                
            } catch (Exception e){
                e.printStackTrace();
                logger.log(Level.SEVERE, "Error parsing date: " + e.getMessage());
                return null;
            }
        } else {
            return null;
        }
    }
    
    synchronized Date getStartDate() throws Exception {
        if(startDate!=null){
            return startDate;
        } else {
            startDate = getDateFormat().parse(startDateText);
            return startDate;
        }
    }
    
    synchronized SimpleDateFormat getDateFormat(){
        if(sdf!=null){
            return sdf;
        } else {
            sdf = new SimpleDateFormat(format);
            return sdf;
        }
    }
}