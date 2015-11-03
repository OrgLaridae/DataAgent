package org.mora.cep.main;

import org.mora.cep.cepProcessing.DataBridgeClient;
import org.mora.cep.cepProcessing.TaskRepeat;
import org.mora.cep.reader.Consumer;

import java.util.Timer;

/**
 * Created by ruveni on 31/10/15.
 */
public class Main {
    public static void main(String args[]){
        DataBridgeClient dataClient=new DataBridgeClient();
        TaskRepeat tr = new TaskRepeat(null, null);
        try{
            dataClient.SendDataToCEP();
        }catch (Exception e){
            e.printStackTrace();
        }

        Timer timer=new Timer();
        try{
            timer.scheduleAtFixedRate(new Consumer(timer, tr.level2Queue),10000,5000);
        }catch (Exception e){
            timer.cancel();
            timer.purge();
            e.printStackTrace();
        }

    }
}
