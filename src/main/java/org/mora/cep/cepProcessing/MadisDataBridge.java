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

    public MadisDataBridge() {
        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();
        //configuration to add siddhi extension
        List extensionClasses = new ArrayList();
        extensionClasses.add(org.mora.cep.sidhdhiExtention.IsNearStation.class);
        extensionClasses.add(org.mora.cep.sidhdhiExtention.IsNearTimestamp.class);

        siddhiConfiguration.setSiddhiExtensions(extensionClasses);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        //stream definitions
        siddhiManager.defineStream("define stream WeatherStream (stationId string, dateTime string, dewTemperature double, relativeHumidity double, seaPressure double, pressure double, temperature double, windDirection double, windSpeed double, latitude double, longitude double) ");
        siddhiManager.defineStream("define stream FilterStream (stationId string, dateTime string,latitude double, longitude double,temperature double) ");

        //execution plan
        //String queryReference = siddhiManager.addQuery("from  WeatherStream[temperature >= 60] select temperature insert into FilterStream ;");
        String anomalyRemover = siddhiManager.addQuery("from  WeatherStream[temperature >= 60] #window.unique(stationId) as A " +
                "join WeatherStream[temperature >= 60] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId " +
                "select A.stationId,A.dateTime,A.latitude,A.longitude,A.temperature " +
                "insert into FilterStream ;");

        //and madis:isNearTimestamp(A.dateTime,B.dateTime) and A.stationId != B.stationId
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
