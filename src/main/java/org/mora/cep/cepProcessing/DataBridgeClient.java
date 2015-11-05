package org.mora.cep.cepProcessing;

import org.mora.cep.util.KeyStoreUtils;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.net.MalformedURLException;

/**
 * Created by chamil on 9/3/15.
 */
public class DataBridgeClient {
    public String streamId = null;
    public DataPublisher dataPublisher = null;

    public void SendDataToCEP(int rowValue, int colValue, double zValue) {
        try {
            KeyStoreUtils.setTrustStoreParams();
            dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
            streamId = dataPublisher.defineStream("{" +
                    " 'name':'RadarStream'," +
                    " 'version':'1.0.0'," +
                    " 'nickName': 'Radar Data Stream'," +
                    " 'description': 'Laridae Radar Data Reader'," +
                    " 'payloadData':[" +
                    "           {'name':'rowValue','type':'INT'}," +
                    "           {'name':'colValue','type':'INT'}," +
                    "           {'name':'zValue','type':'DOUBLE'}" +
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
                dataPublisher.publish(streamId, new Object[]{"127.0.0.1"}, null, new Object[]{rowValue, colValue, zValue});
            } catch (Exception e) {
                e.printStackTrace();
            }

//            Timer timer = new Timer();
//            try{
//                timer.scheduleAtFixedRate(new TaskRepeat(streamId, dataPublisher), 5000, 5000);
//            }catch (Exception e){
//                timer.cancel();
//                timer.purge();
//            }
//            Timer timer = new Timer();
//            try{
//                timer.scheduleAtFixedRate(new TaskRepeat(streamId, dataPublisher), 5000, 5000);
//            }catch (Exception e){
//                timer.cancel();
//                timer.purge();
//            }
        }
    }
}