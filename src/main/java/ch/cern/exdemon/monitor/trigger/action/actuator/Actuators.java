package ch.cern.exdemon.monitor.trigger.action.actuator;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class Actuators {

    public static void run(JavaDStream<Action> actions, Properties propertiesSourceProps) {
        actions.foreachRDD(rdd -> rdd.foreach(new RunActuatorsF(propertiesSourceProps)));
    }

}
