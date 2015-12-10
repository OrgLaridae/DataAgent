package org.mora.cep.sidhdhiExtention;

import org.apache.log4j.helpers.DateTimeDateFormat;
import org.springframework.format.annotation.DateTimeFormat;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.time.format.DateTimeFormatter;

/**
 * Created by chamil on 12/9/15.
 */

// incomplete
@SiddhiExtension(namespace = "madis", function = "boundary")
public class MadisBoundry extends FunctionExecutor {


    protected Object process(double lat, double lon, String timestamp) {
        DateTime benchMarkTime = null;
        double minLat=0, minLon=0,maxLat=0, maxLon =0;
        String boundry;

        if(benchMarkTime== null){
            benchMarkTime = getTimestamp(timestamp);
            minLat = maxLat = lat;
            minLon = maxLon = lon;

        }
        else if(getTimestamp(timestamp).after(benchMarkTime.plusTime(15))){ // should change
           benchMarkTime = getTimestamp(timestamp);
            boundry = minLat + " " + maxLat + " " + minLon + " " + maxLon;
            minLat = maxLat = lat;
            minLon = maxLon = lon;
            return boundry;
        }
        else{
            if (lat > maxLat) {
                maxLat = lat;
            }
            if (lat < minLat) {
                minLat = lat;
            }
            if (lon > maxLon) {
                maxLon = lon;
            }
            if (lon < minLon) {
                minLon = lon;
            }
        }
        return null;
    }

    private DateTime getTimestamp(String dateString){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss"); // or is it HH:mm only?
        DateTime dt = formatter.parseDateTime(string);
        return dt;
    }

    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {

    }

    @Override
    protected Object process(Object o) {
        return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }
}
