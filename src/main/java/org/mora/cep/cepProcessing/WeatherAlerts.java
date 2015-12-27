package org.mora.cep.cepProcessing;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by ruveni on 15/11/15.
 */
public class WeatherAlerts {
    private SiddhiManager siddhiManager;
    private InputHandler madisInputHandler;
    private InputHandler radarInputHandler;
    private static final double THRESHOLD_TEMPERATURE=60;//in Kelvin
    private static final double THRESHOLD_WIND=70; //in mph
    private static final int TIME_GAP=1; //in minutes

    public WeatherAlerts(SiddhiManager siddhiManager) {
        this.siddhiManager=siddhiManager;
        madisInputHandler = siddhiManager.getInputHandler("WeatherStream");
        radarInputHandler = siddhiManager.getInputHandler("reflectStream");
        calculateBoundary();
        highTemperatureAlert();
        radarDataBoundary();
    }

    public void SendDataToCEP(String stationId, String dateTime, double dewTemperature, double relativeHumidity, double seaPressure, double pressure, double temperature, double windDirection, double windSpeed, double latitude, double longitude) {
        try {
            madisInputHandler.send(new Object[]{stationId, dateTime,dewTemperature, relativeHumidity, seaPressure, pressure, temperature, windDirection, windSpeed, latitude, longitude});
        } catch (Exception e) {

        }
    }

    public void highTemperatureAlert(){
        String anomalyRemover = siddhiManager.addQuery("from  WeatherStream[temperature >= "+THRESHOLD_TEMPERATURE+"] #window.unique(stationId) as A " +
                "join WeatherStream[temperature >= "+THRESHOLD_TEMPERATURE+"] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilterStream ;");

        siddhiManager.addCallback(anomalyRemover, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Madis Temperature: ");
                EventPrinter.print(inEvents);
            }
        });
    }

    public void highWindAlert(){
        String highWindDetect = siddhiManager.addQuery("from  WeatherStream[windSpeed >= "+THRESHOLD_WIND+"] #window.unique(stationId) as A " +
                "join WeatherStream[windSpeed >= "+THRESHOLD_WIND+"] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilterStream ;");

        siddhiManager.addCallback(highWindDetect, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Madis Wind : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

    }

    public void calculateBoundary(){
        String calBoundary=siddhiManager.addQuery("from FilterStream #window.timeBatch( "+TIME_GAP+" min ) "+
                "select min(latitude) as minLatitude, max(latitude) as maxLatitude, min(longitude) as minLongitude, max(longitude) as maxLongitude, count(stationId) as dataCount "+
                "insert into DataBoundary for all-events ; ");

        siddhiManager.addCallback(calBoundary, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Madis Boundary : ");
                EventPrinter.print(inEvents);
            }
        });
    }

    //radar data processing
    public void radarDataBoundary(){
        String queryReference = siddhiManager.addQuery("from reflectStream select file:getPath(reflexMatrix) as filePath, radar:boundary(reflexMatrix) as boundary insert into boundaryStream ;");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Radar : ");
                EventPrinter.print(inEvents);
            }
        });
    }

    public void SendDataToCEP(String matrix) {
        try {
            radarInputHandler.send(new Object[]{matrix});
        }catch (Exception e){

        }
    }


}
