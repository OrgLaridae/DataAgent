package org.mora.cep.cepProcessing;

import org.mora.cep.util.KeyStoreUtils;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.net.MalformedURLException;

/**
 * Created by ruveni on 15/11/15.
 */
public class MadisDataBridge {
    public String streamId = null;
    public DataPublisher dataPublisher = null;

    public void SendDataToCEP(double dewTemperature, double relativeHumidity, double seaPressure, double pressure, double temperature, double windDirection, double windSpeed, double latitude, double longitude) {
        try {
            KeyStoreUtils.setTrustStoreParams();
            dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
            streamId = dataPublisher.defineStream("{" +
                    " 'name':'WeatherStream'," +
                    " 'version':'1.0.0'," +
                    " 'nickName': ''," +
                    " 'description': ''," +
                    " 'payloadData':[" +
                    "           {'name':'dewTemperature','type':'DOUBLE'}," +
                    "           {'name':'relativeHumidity','type':'DOUBLE'}," +
                    "           {'name':'seaPressure','type':'DOUBLE'}," +
                    "           {'name':'pressure','type':'DOUBLE'}," +
                    "           {'name':'temperature','type':'DOUBLE'}," +
                    "           {'name':'windDirection','type':'DOUBLE'}," +
                    "           {'name':'windSpeed','type':'DOUBLE'}," +
                    "           {'name':'latitude','type':'DOUBLE'}," +
                    "           {'name':'longitude','type':'DOUBLE'}" +

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
            try {
                dataPublisher.publish(streamId, null, null, new Object[]{dewTemperature,relativeHumidity,seaPressure,pressure,temperature,windDirection,windSpeed,latitude,longitude});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
