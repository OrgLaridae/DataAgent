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

    public void SendDataToCEP(String matrix) {
        try {
            KeyStoreUtils.setTrustStoreParams();
            dataPublisher = new DataPublisher("tcp://localhost:7611", "admin", "admin");
            streamId = dataPublisher.defineStream("{" +
                    " 'name':'ReflectStream'," +
                    " 'version':'1.0.0'," +
                    " 'nickName': ''," +
                    " 'description': ''," +
                    " 'payloadData':[" +
                    "           {'name':'reflexMatrix','type':'STRING'}" +
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
                dataPublisher.publish(streamId, null, null, new Object[]{matrix});
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