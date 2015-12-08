package org.mora.cep.main;

import org.mora.cep.cepProcessing.CEPInitialize;
import org.mora.cep.cepProcessing.DataFeed;
import org.mora.cep.cepProcessing.MadisDataFeed;
import org.wso2.siddhi.core.SiddhiManager;

/**
 * Created by ruveni on 31/10/15.
 */
public class Main {
    public static void main(String args[]) {
        CEPInitialize cep=new CEPInitialize();
        SiddhiManager siddhiManager=cep.CEPInit();
        //radar data feed
        DataFeed dataFeed = new DataFeed(siddhiManager);
        Thread feedThread = new Thread(dataFeed);
        feedThread.start();

        //madis data feed
        MadisDataFeed madisDataFeed=new MadisDataFeed(siddhiManager);
        Thread madisThread=new Thread(madisDataFeed);
        madisThread.start();
    }
}