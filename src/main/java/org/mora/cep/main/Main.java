package org.mora.cep.main;

import org.mora.cep.cepProcessing.DataFeed;
import org.mora.cep.cepProcessing.MadisDataFeed;

/**
 * Created by ruveni on 31/10/15.
 */
public class Main {
    public static void main(String args[]) {
        //radar data feed
        DataFeed dataFeed = new DataFeed();
        Thread feedThread = new Thread(dataFeed);
        feedThread.start();

        //madis data feed
        MadisDataFeed madisDataFeed=new MadisDataFeed();
        Thread madisThread=new Thread(madisDataFeed);
        madisThread.start();
    }
    //bhjgj
}