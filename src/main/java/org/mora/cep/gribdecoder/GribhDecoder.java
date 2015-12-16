package org.mora.cep.gribdecoder;

/**
 * Created by chamil on 12/16/15.
 */

import net.sourceforge.jgrib.GribFile;
import net.sourceforge.jgrib.NoValidGribException;
import net.sourceforge.jgrib.NotSupportedException;

import java.io.FileNotFoundException;
import java.io.IOException;

public class GribhDecoder {


    GribFile gribFile;
    public GribhDecoder(String path){

    try{
        // Opening GRIB File
        this.gribFile = new GribFile(path);

    } catch (FileNotFoundException noFileError) {
        System.err.println("FileNotFoundException : " + noFileError);
    } catch (IOException e) {
        e.printStackTrace();
    } catch (NoValidGribException e) {
        e.printStackTrace();
    } catch (NotSupportedException e) {
        e.printStackTrace();
    }
    }
}
