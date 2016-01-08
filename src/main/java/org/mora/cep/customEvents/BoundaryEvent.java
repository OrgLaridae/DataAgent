package org.mora.cep.customEvents;

import java.util.EventObject;

/**
 * Created by ruveni on 1/8/16.
 */
public class BoundaryEvent extends EventObject {
    private String boundary;
    public BoundaryEvent(Object source, String boundary){
        super(source);
        this.boundary=boundary;
    }

    public String getBoundary(){
        return boundary;
    }
}
