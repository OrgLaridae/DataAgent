package org.mora.cep.main;

import org.mora.cep.cepProcessing.CEPInitialize;
import org.mora.cep.dataFeed.CSVFileReader;
import org.mora.cep.cepProcessing.WeatherAlerts;
import org.wso2.siddhi.core.SiddhiManager;

/**
 * Created by ruveni on 31/10/15.
 */
public class CEP {
    public static void main(String args[]) {
        CEPInitialize cep=new CEPInitialize();
        SiddhiManager siddhiManager=cep.CEPInit();
        WeatherAlerts weatherAlerts=new WeatherAlerts(siddhiManager);

        //csv data feed - grib data
        CSVFileReader csvReader=new CSVFileReader(weatherAlerts);
        Thread csvThread=new Thread(csvReader);
        csvThread.start();
    }
}