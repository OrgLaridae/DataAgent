package org.mora.cep.sidhdhiExtention;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

/**
 * Created by ruveni on 08/12/15.
 */

//latitudes and longitude values of two different stations are passed as parameters
//calculates the distance in between the two points
//checks the calculated distance is more than a defined threshold value
@SiddhiExtension(namespace = "madis", function = "isNearStation")
public class IsNearStation extends FunctionExecutor {
    Attribute.Type returnType;
    private static final double DISTANCE_THRESHOLD = 100;
    private static final double EARTH_RADIUS = 6371;//in kilometers

    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
        returnType = Attribute.Type.BOOL;
        for (Attribute.Type attributeType : types) {
            if (attributeType == Attribute.Type.DOUBLE) {
                break;
            } else {
                throw new QueryCreationException("Attributes should be type of DOUBLE");
            }
        }
    }

    @Override
    protected Object process(Object o) {
        boolean isNear = false;
        if ((o instanceof Object[]) && ((Object[]) o).length == 4) {
            double latA=Double.parseDouble(String.valueOf(((Object[]) o)[0]));
            double lonA=Double.parseDouble(String.valueOf(((Object[]) o)[1]));
            double latB=Double.parseDouble(String.valueOf(((Object[]) o)[2]));
            double lonB=Double.parseDouble(String.valueOf(((Object[]) o)[3]));

            double dLat = Math.toRadians(latB - latA);
            double dLng = Math.toRadians(lonB - lonA);

            //calculates the actual distance
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(latA))* Math.cos(Math.toRadians(latB)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double dist = (double) (EARTH_RADIUS * c)*1000;

            if(dist < DISTANCE_THRESHOLD){
                isNear=true;
            }

        }
        return isNear;
    }

    @Override
    public void destroy() {

    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }
}
