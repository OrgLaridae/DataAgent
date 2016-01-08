package org.mora.cep.customEvents;

import java.util.EventObject;

/**
 * Created by ruveni on 1/8/16.
 */
public class AlertEvent  extends EventObject{
    private String location;
    public AlertEvent(Object source, String coordinates){
        super(source);
        location=coordinates;
    }

    public String getCoordinates(){
        return location;
    }
}
