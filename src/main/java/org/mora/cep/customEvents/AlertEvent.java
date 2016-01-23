package org.mora.cep.customEvents;

import java.util.ArrayList;
import java.util.EventObject;

/**
 * Created by ruveni on 1/8/16.
 */
public class AlertEvent  extends EventObject{
    private ArrayList<Location> location;
    public AlertEvent(Object source, ArrayList<Location> coordinates){
        super(source);
        location=coordinates;
    }

    public ArrayList<Location> getCoordinates(){
        return location;
    }
}
