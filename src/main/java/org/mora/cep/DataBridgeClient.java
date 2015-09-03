package org.mora.cep;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mora.cep.util.KeyStoreUtils;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by chamil on 9/3/15.
 */
public class DataBridgeClient {

    public static void main(String[] args) {

        DataPublisher dataPublisher = null;
        String streamId1 = null;
        try {

            KeyStoreUtils.setTrustStoreParams();
            dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
            streamId1 = dataPublisher.defineStream("{" +
                    " 'name':'WeatherStream'," +
                    " 'version':'1.0.0'," +
                    " 'nickName': 'Weather Data Stream'," +
                    " 'description': 'Some Desc'," +
                    " 'metaData':[" +
                    "           {'name':'ipAdd','type':'STRING'}" +
                    " ]," +
                    " 'payloadData':[" +
                    "           {'name':'temperature','type':'DOUBLE'}," +
                    "           {'name':'pressure','type':'DOUBLE'}," +
                    "           {'name':'humidity','type':'DOUBLE'}," +
                    "           {'name':'windSpeed','type':'DOUBLE'}," +
                    "           {'name':'windDirection','type':'DOUBLE'}" +
                    " ]" +
                    "}");
        } catch (AgentException e) {
            e.printStackTrace();
        } catch (MalformedStreamDefinitionException e) {
            e.printStackTrace();
        } catch (StreamDefinitionException e) {
            e.printStackTrace();
        } catch (DifferentStreamDefinitionAlreadyDefinedException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (AuthenticationException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        }

        //In this case correlation data is null
        if (dataPublisher != null) {

            //dataPublisher.publish(streamId1, new Object[]{"127.0.0.1"}, null, new Object[]{"IBM", 96.8, 300, 120.6, 70.4});
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TaskRepeat(streamId1, dataPublisher), 5000, 5000);
        }
    }
}

class TaskRepeat extends TimerTask {
    String streamID = "";
    DataPublisher dataPublisher;

    TaskRepeat(String streamID, DataPublisher datPublisher) {
        this.streamID = streamID;
        this.dataPublisher = datPublisher;
    }

    public void run() {
        FetchData(streamID, dataPublisher);
    }

    public void FetchData(String streamID, DataPublisher dataPublisher) {
        try {
            URL url = new URL("http://api.openweathermap.org/data/2.5/weather?lat=35&lon=139");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            String finalOutput = "";
            //System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                //System.out.println(output);
                finalOutput = output;
            }

            conn.disconnect();

            if (finalOutput != null) {
                JSONParser parser = new JSONParser();
                Object obj = parser.parse(finalOutput);
                JSONObject jsonObj = (JSONObject) obj;

                String temperature = " ", pressure = " ", humidity = " ", windSpeed = " ", windDirection = " ";

                String temperatureData = jsonObj.get("main").toString();
                String windData = jsonObj.get("wind").toString();
                String cloudsData = jsonObj.get("clouds").toString();

                JSONObject tempObj = (JSONObject) parser.parse(temperatureData);
                try {
                    temperature = tempObj.get("temp").toString();
                } catch (Exception e) {
                    temperature = "0.0";
                }
                try {
                    pressure = tempObj.get("pressure").toString();
                } catch (Exception e) {
                    pressure = "0.0";
                }
                try {
                    humidity = tempObj.get("humidity").toString();
                } catch (Exception e) {
                    humidity = "0.0";
                }

                JSONObject windObject = (JSONObject) parser.parse(windData);
                try {
                    windSpeed = windObject.get("speed").toString();
                } catch (Exception e) {
                    windSpeed = "0.0";
                }
                try {
                    windDirection = windObject.get("deg").toString();
                } catch (Exception e) {
                    windDirection = "0.0";
                }

//                JSONObject cloudObject = (JSONObject) parser.parse(cloudsData);
//                String cloudsAll = cloudObject.get("all").toString();
                System.out.println(temperature + " " + pressure + " " + humidity + " " + windSpeed + " " + windDirection);
                dataPublisher.publish(streamID, new Object[]{"127.0.0.1"}, null, new Object[]{Double.parseDouble(temperature), Double.parseDouble(pressure), Double.parseDouble(humidity), Double.parseDouble(windSpeed), Double.parseDouble(windDirection)});
            }
            //String finalOutput="{\"coord\":{\"lon\":139.78,\"lat\":38.82},\"weather\":[{\"id\":803,\"main\":\"Clouds\",\"description\":\"broken clouds\",\"icon\":\"04n\"}],\"base\":\"stations\",\"main\":{\"temp\":295.54,\"pressure\":1009,\"humidity\":73,\"temp_min\":295.15,\"temp_max\":296.15},\"visibility\":10000,\"wind\":{\"speed\":2.1,\"deg\":90},\"clouds\":{\"all\":75},\"dt\":1441274400,\"sys\":{\"type\":1,\"id\":7605,\"message\":0.0113,\"country\":\"JP\",\"sunrise\":1441224678,\"sunset\":1441271321},\"id\":1863282,\"name\":\"Hamanaka\",\"cod\":200}";
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
