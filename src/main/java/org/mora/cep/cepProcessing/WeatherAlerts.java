package org.mora.cep.cepProcessing;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by ruveni on 15/11/15.
 */
/*
    Necessary GRIB parameters
    =================================
    Best 4 layer lifted index @ Layer between 2 level at pressure difference from ground to level layer (K)
    Dew point temperature @ Isobaric surface (K) 850hPa
    Dew point temperature @ Isobaric surface (K) 700hPa
    Dew point temperature @ Isobaric surface (K) 500hPa
    Temperature @ Isobaric surface (K) 500hPa
    Temperature @ Isobaric surface (K) 850hPa
    Temperature @ Isobaric surface (K) 700hPa
    Storm relative helicity @ Layer between 2 specified height level above ground layer (m2/s2) 500m
    Convective inhibition @ Ground or water surface (J/kg)
    Precipitable water @ Entire atmosphere (kg/m2)
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
        String highWindDetect = siddhiManager.addQuery("from WeatherStream[windSpeed >= "+THRESHOLD_WIND+"] #window.unique(stationId) as A " +
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

    //TEMPERATURE AND HUMIDITY RELATED INDICES

    //gets the Lifted Index and checks whether it is positive or negative
    //If the value is negative, the probability of occuring a thunderstorm is greater
    public void checkLiftedIndex(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream [liftedIndex<0] #window.unique(stationId) as A " +
                "join WeatherStream[liftedIndex<0] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select 'A' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Lifted Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //Total Totals Index
    //temperature parameters in Kelvin
    //TT > 44 = possible thunderstorms, slight chance of severe TT > 50 = moderate chance of severe thunderstorms TT > 55 = strong chance of severe thunderstorms
    //TT = Td850 + T850 - 2(T500) or (Td850 - T500) + (T850 - T500)
    public void checkTotalsIndex(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream #window.unique(stationId) as A " +
                "join WeatherStream #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) and (A.dewTemp850 + A.temp850 - 2*A.temp500) > 44 " +
                "select 'B' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Totals Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //K Index
    //K = T850 - T500 + Td850 - (T700 - Td700)
    //temperature parameters in Kelvin
    //K==26-30 40-60% Air mass thunderstorm probability
    public void checkKIndex(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream #window.unique(stationId) as A " +
                "join WeatherStream #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) and (A.temp850 - A.temp500 + A.dewTemp850 - (A.temp700 - A.dewTemp700)) > 25 " +
                "select 'C' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("K Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //Humidity Index HI
    //HI = (T 850 − T d,850 ) + (T 700 − T d,700 ) + (T 500 − T d,500 )
    //Threshold is 30K - Temperature parameters in Kelvin

    public void checkHumidityIndex(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream #window.unique(stationId) as A " +
                "join WeatherStream #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) and ((A.temp850 - A.dewTemp850) + (A.temp700 - A.dewTemp700) + (A.temp500 - A.dewTemp500)) < 30 " +
                "select 'D' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("K Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //WIND RELATED INDICES

    //Storm Relative Helicity SRH
    //Storm relative helicity @ Layer between 2 specified height level above ground layer (m2/s2)
    //threshold value is 150 m2/s2

    public void checkHelicity(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream [stormHelicity>150] #window.unique(stationId) as A " +
                "join WeatherStream[stormHelicity>150] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select 'E' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Helicity Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //COMPLEX PARAMETERS

    //Convective Inhibition - CIN
    //Convective inhibition @ Ground or water surface (J/kg) parameter
    //threshold value is 15J/kg

    public void checkInhibition(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream [convectiveInhibition<(-15)] #window.unique(stationId) as A " +
                "join WeatherStream[convectiveInhibition<(-15)] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select 'F' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Convective Inhibition : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    //PRECIPITABLE WATER (PW) (kgm-2 or mm)
    //Precipitable water @ Entire atmosphere (kg/m2)
    //Threshold is 16mm

    public void checkPecipitableWater(){
        String checkIndex=siddhiManager.addQuery("from WeatherStream [precipitableWater>16] #window.unique(stationId) as A " +
                "join WeatherStream[precipitableWater>16] #window.unique(stationId) as B " +
                "on madis:isNearStation(A.latitude,A.longitude,B.latitude,B.longitude) and A.stationId != B.stationId and madis:isNearTimestamp(A.dateTime,B.dateTime) " +
                "select 'G' as streamID, A.stationId,A.dateTime,A.latitude,A.longitude " +
                "insert into FilteredStream ;");

        siddhiManager.addCallback(checkIndex, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Precipitable Water Index : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
    }

    public void precipitableWaterBoundary(){
        String calBoundary=siddhiManager.addQuery("from PrecipitableWaterStream #window.timeBatch( "+TIME_GAP+" min ) "+
                "select min(latitude) as minLatitude, max(latitude) as maxLatitude, min(longitude) as minLongitude, max(longitude) as maxLongitude, count(stationId) as dataCount "+
                "insert into DataBoundary for all-events ; ");

        siddhiManager.addCallback(calBoundary, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Precipitable Water Boundary : ");
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

    //calculate the common boundary
    public void calculateCommonBoundary(){
        String calBoundary=siddhiManager.addQuery("from FilteredStream #window.unique(streamID) "+
                "select min(latitude) as minLatitude, max(latitude) as maxLatitude, min(longitude) as minLongitude, max(longitude) as maxLongitude, count(stationId) as dataCount "+
                "insert into DataBoundary for all-events ; ");

        siddhiManager.addCallback(calBoundary, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Resulting Boundary : ");
                EventPrinter.print(inEvents);
            }
        });
    }
}
