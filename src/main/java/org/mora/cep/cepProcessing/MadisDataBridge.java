package org.mora.cep.cepProcessing;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;


/**
 * Created by ruveni on 15/11/15.
 */
public class MadisDataBridge {
    SiddhiManager siddhiManager;
    InputHandler inputHandler;

    public MadisDataBridge(){
        siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream WeatherStream (dewTemperature double, relativeHumidity double, seaPressure double, pressure double, temperature double, windDirection double, windSpeed double, latitude double, longitude double) ");
        siddhiManager.defineStream("define stream FilterStream (temperature double) ");
        String queryReference = siddhiManager.addQuery("from  WeatherStream[ temperature >= 60] select temperature insert into FilterStream ;");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Madis : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        inputHandler = siddhiManager.getInputHandler("WeatherStream");
    }

    public void SendDataToCEP(double dewTemperature, double relativeHumidity, double seaPressure, double pressure, double temperature, double windDirection, double windSpeed, double latitude, double longitude) {
        try {
            inputHandler.send(new Object[]{dewTemperature,relativeHumidity,seaPressure,pressure,temperature,windDirection,windSpeed,latitude,longitude});
        }catch (Exception e){

        }
    }
}
