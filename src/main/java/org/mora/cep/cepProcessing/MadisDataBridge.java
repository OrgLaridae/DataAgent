package org.mora.cep.cepProcessing;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by ruveni on 15/11/15.
 */
public class MadisDataBridge {
    SiddhiManager siddhiManager;
    InputHandler inputHandler;

    public MadisDataBridge(SiddhiManager siddhiManager) {
        this.siddhiManager=siddhiManager;

        String anomalyRemover = siddhiManager.addQuery("from  WeatherStream[temperature >= 60] #window.unique(stationId) as A " +
                "join WeatherStream[temperature >= 60] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select A.stationId,A.dateTime,A.latitude,A.longitude,A.temperature " +
                "insert into FilterStream ;");

        siddhiManager.addCallback(anomalyRemover, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Madis : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        inputHandler = siddhiManager.getInputHandler("WeatherStream");
    }

    public void SendDataToCEP(String stationId, String dateTime, double dewTemperature, double relativeHumidity, double seaPressure, double pressure, double temperature, double windDirection, double windSpeed, double latitude, double longitude) {
        try {
            inputHandler.send(new Object[]{stationId, dateTime,dewTemperature, relativeHumidity, seaPressure, pressure, temperature, windDirection, windSpeed, latitude, longitude});
        } catch (Exception e) {

        }
    }
}
