package org.mora.cep.cepProcessing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ruveni on 04/11/15.
 */
public class DataFeed implements Runnable {
    DataBridgeClient dataBridgeClient=null;
    //Producer producer=null;
    StringBuilder message=null;
    String filePath="/home/ruveni/ekxv0000.txt";

    @Override
    public void run() {
        double[][] val = dBZToZ(filePath);
        dataBridgeClient=new DataBridgeClient();
        dataBridgeClient.SendDataToCEP(message.toString());
//        producer=new Producer(message);
//        producer.sendMessage();
    }

    public double[][] dBZToZ(String location){
        message = new StringBuilder();
        message.append(filePath);
        double[][] Z = new double[240][240];
        double alpha = 0.5;
        double beta = -32;
        Path path = Paths.get(location);
        try (Stream<String> lines = Files.lines(path)) {
            String[] lineArray = lines.collect(Collectors.toList()).toArray(new String[0]);
            for (int i = 0; i < lineArray.length; i++) {
                String[] stringData = lineArray[i].split(",");
                for (int j = 0; j < 240; j++) {
                    double data = Double.parseDouble(stringData[j]);
                    data = (data == 255.0) ? 0 : data;
                    data = (alpha * data) + beta;
                    data = Math.pow(10, (data / 10));
                    //data=(data<1)?0:Math.round(data * 10000.0) / 10000.0;
                    Z[i][j] = data;
                    message.append("," + Z[i][j]);
                }
            }
        } catch (IOException ex) {

        }
        return Z;
    }
}