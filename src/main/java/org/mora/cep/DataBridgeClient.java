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
