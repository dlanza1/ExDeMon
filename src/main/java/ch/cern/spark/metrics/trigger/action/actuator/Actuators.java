package ch.cern.spark.metrics.trigger.action.actuator;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.trigger.action.Action;

public class Actuators {

    public static final String PARAM = "actuators";

    public static void run(JavaDStream<Action> actions, Properties propertiesSourceProps) {
        actions.foreachRDD(rdd -> rdd.foreach(new RunActuatorsF(propertiesSourceProps)));
    }

}
