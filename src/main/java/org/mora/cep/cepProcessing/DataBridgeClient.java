package org.mora.cep.cepProcessing;

import org.mora.cep.util.KeyStoreUtils;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.net.MalformedURLException;
import java.util.Timer;

/**
 * Created by chamil on 9/3/15.
 */
public class DataBridgeClient {
    public String streamId=null;
    public DataPublisher dataPublisher=null;
    public void SendDataToCEP(){
        try {

            KeyStoreUtils.setTrustStoreParams();
            dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
            streamId = dataPublisher.defineStream("{" +
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
                    "           {'name':'latitude','type':'DOUBLE'}," +
                    "           {'name':'longitude,'type':'DOUBLE'}" +
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
            try{
                timer.scheduleAtFixedRate(new TaskRepeat(streamId, dataPublisher), 5000, 5000);
            }catch (Exception e){
                timer.cancel();
                timer.purge();
            }
        }
    }
}
