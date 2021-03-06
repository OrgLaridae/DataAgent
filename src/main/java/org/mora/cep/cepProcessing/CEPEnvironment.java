package org.mora.cep.cepProcessing;

/**
 * Created by ruveni on 1/21/16.
 */
public class CEPEnvironment {
    public static final int TIME_GAP = 1; //in minutes
    public static final int THRESHOLD_LIFTED_INDEX=-1;
    public static final int THRESHOLD_HELICITY=150;
    public static final int THRESHOLD_INHIBITION=-50;
    public static final String LIFTED_INDEX_FILE_PATH="/home/ruveni/IdeaProjects/DataAgent/src/main/java/org/mora/cep/csvFiles/Best(4layer)liftedindex.csv";
    public static final String HELICITY_INDEX_FILE_PATH="/home/ruveni/IdeaProjects/DataAgent/src/main/java/org/mora/cep/csvFiles/stormrelativehelicity.csv";
    public static final String CONVECTIVE_INHIBITION_FILE_PATH="/home/ruveni/IdeaProjects/DataAgent/src/main/java/org/mora/cep/csvFiles/convectiveInhibition.csv";
    public static final double DISTANCE_THRESHOLD = 40;
    public static final String COORDINATE_FILE_PATH="/home/ruveni/Data/Test.txt";
}
