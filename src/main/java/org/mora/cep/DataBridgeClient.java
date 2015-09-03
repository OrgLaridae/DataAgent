package org.mora.cep;

import org.mora.cep.util.KeyStoreUtils;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.exception.*;

import java.net.MalformedURLException;

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
                    " 'name':'org.wso2.esb.MediatorStatistics'," +
                    " 'version':'2.3.0'," +
                    " 'nickName': 'Stock Quote Information'," +
                    " 'description': 'Some Desc'," +
                    " 'metaData':[" +
                    "           {'name':'ipAdd','type':'STRING'}" +
                    " ]," +
                    " 'payloadData':[" +
                    "           {'name':'symbol','type':'STRING'}," +
                    "           {'name':'price','type':'DOUBLE'}," +
                    "           {'name':'volume','type':'INT'}," +
                    "           {'name':'max','type':'DOUBLE'}," +
                    "           {'name':'min','type':'Double'}" +
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
        if (dataPublisher != null){
            try {
                dataPublisher.publish(streamId1, new Object[]{"127.0.0.1"}, null, new Object[]{"IBM", 96.8, 300, 120.6, 70.4});
            } catch (AgentException e) {
                e.printStackTrace();
            }
            //Only call this before shutting down the client
            dataPublisher.stop();
        }



    }


}
