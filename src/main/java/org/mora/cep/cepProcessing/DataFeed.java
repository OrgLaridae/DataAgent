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
    private StringBuilder message=null;
    String filePath="/home/ruveni/ekxv0000.txt";

    @Override
    public void run() {
        //reads the input file and store the contents as a string
        readFile(filePath);
        dataBridgeClient=new DataBridgeClient();
        //pass the file content to CEP for processing
        dataBridgeClient.SendDataToCEP(message.toString());
    }

    public void readFile(String location){
        message = new StringBuilder();
        message.append(filePath+",");
        Path path = Paths.get(location);
        try (Stream<String> lines = Files.lines(path)) {
            String[] lineArray = lines.collect(Collectors.toList()).toArray(new String[0]);
            for(String line:lineArray){
                message.append(line);
            }
        } catch (IOException ex) {

        }
    }
}