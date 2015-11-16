package org.mora.cep.cepProcessing;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by chamil on 9/3/15.
 */
public class DataBridgeClient {
    private SiddhiManager siddhiManager;
    private InputHandler inputHandler;

    public DataBridgeClient(){
        siddhiManager = new SiddhiManager();

        //configuration to add siddhi extension
        List extensionClasses = new ArrayList();
        extensionClasses.add(org.mora.cep.sidhdhiExtention.CalculateBoundary.class);
        extensionClasses.add(org.mora.cep.sidhdhiExtention.RadarFilePath.class);

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();
        siddhiConfiguration.setSiddhiExtensions(extensionClasses);

        SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);

        siddhiManager.defineStream("define stream reflectStream (reflexMatrix string )  ");
        siddhiManager.defineStream("define stream boundaryStream ( filePath string, boundary string )  ");
        String queryReference = siddhiManager.addQuery("from reflectStream select file:getPath(reflexMatrix) as filePath, radar:boundary(reflexMatrix) as boundary insert into boundaryStream ;");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.print("Radar : ");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        inputHandler = siddhiManager.getInputHandler("reflectStream");
    }

    public void SendDataToCEP(String matrix) {
        try {
            inputHandler.send(new Object[]{matrix});
        }catch (Exception e){

        }
    }
}