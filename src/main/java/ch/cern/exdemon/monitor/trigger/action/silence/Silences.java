package ch.cern.exdemon.monitor.trigger.action.silence;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class Silences {

    public static JavaDStream<Action> apply(JavaDStream<Action> actions, Properties componentsSourceProps) {
        return actions.filter(new SilencesFilterF(componentsSourceProps));
    }

}
