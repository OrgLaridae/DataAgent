package org.mora.cep.cepProcessing;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.TimerTask;

/**
 * Created by ruveni on 9/4/15.
 */
public class TaskRepeat extends TimerTask {
    public String streamID = "";
    public DataPublisher dataPublisher;

    TaskRepeat(String streamID, DataPublisher datPublisher) {
        this.streamID = streamID;
        this.dataPublisher = datPublisher;
    }

    public void run() {
        FetchData(streamID, dataPublisher);
    }

    public void FetchData(String streamID, DataPublisher dataPublisher) {
        try {
            URL url = new URL("http://api.openweathermap.org/data/2.5/weather?lat=35&lon=139&appid=1f82d8351be4f003df99645fb416897f");
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

                String temperature = " ", pressure = " ", humidity = " ", windSpeed = " ", windDirection = " ", latitude=" ", longitude=" ";

                String temperatureData = jsonObj.get("main").toString();
                String coordData = jsonObj.get("coord").toString();
                String windData = jsonObj.get("wind").toString();
                String cloudsData = jsonObj.get("clouds").toString();

                JSONObject coordObj=(JSONObject)parser.parse(coordData);
                try{
                    latitude=coordObj.get("lat").toString();
                }catch (Exception e){
                    latitude="0.0";
                }
                try{
                    longitude=coordObj.get("lon").toString();
                }catch (Exception e){
                    longitude="0.0";
                }

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
                System.out.println(temperature + " " + pressure + " " + humidity + " " + windSpeed + " " + windDirection+" "+latitude+" "+longitude);
                dataPublisher.publish(streamID, new Object[]{"127.0.0.1"}, null, new Object[]{Double.parseDouble(temperature), Double.parseDouble(pressure), Double.parseDouble(humidity), Double.parseDouble(latitude), Double.parseDouble(longitude)});
            }
            //String finalOutput="{\"coord\":{\"lon\":139.78,\"lat\":38.82},\"weather\":[{\"id\":803,\"main\":\"Clouds\",\"description\":\"broken clouds\",\"icon\":\"04n\"}],\"base\":\"stations\",\"main\":{\"temp\":295.54,\"pressure\":1009,\"humidity\":73,\"temp_min\":295.15,\"temp_max\":296.15},\"visibility\":10000,\"wind\":{\"speed\":2.1,\"deg\":90},\"clouds\":{\"all\":75},\"dt\":1441274400,\"sys\":{\"type\":1,\"id\":7605,\"message\":0.0113,\"country\":\"JP\",\"sunrise\":1441224678,\"sunset\":1441271321},\"id\":1863282,\"name\":\"Hamanaka\",\"cod\":200}";
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
