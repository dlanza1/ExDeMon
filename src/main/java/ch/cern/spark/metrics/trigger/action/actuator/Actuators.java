package ch.cern.spark.metrics.trigger.action.actuator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.Cache;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.trigger.action.Action;

public class Actuators {

    private transient final static Logger LOG = Logger.getLogger(Actuators.class.getName());

    public static final String PARAM = "actuators";

    private static final Cache<Map<String, Actuator>> actuators = new Cache<Map<String, Actuator>>() {

        @Override
        protected Map<String, Actuator> load() throws Exception {
            Properties properties = Properties.getCache().get().getSubset(PARAM);

            Set<String> actuatorIDs = properties.getIDs();
            Map<String, Actuator> actuators = actuatorIDs.stream().map(id -> {
                try {
                    Actuator actuator = ComponentManager.build(Type.ACTUATOR, id, properties.getSubset(id));

                    return actuator;
                } catch (Exception e) {
                    LOG.error("ID " + id + ", error when building component: " + e.getMessage(), e);

                    return null;
                }
            }).filter(actuator -> actuator != null).collect(Collectors.toMap(Actuator::getId, a -> a));

            LOG.info("Actuators: " + actuators);

            return actuators;
        }
    };

    public static Cache<Map<String, Actuator>> getCache() {
        return actuators;
    }

    public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
        Properties.initCache(propertiesSourceProps);

        getCache().setExpiration(Properties.getCache().getExpirationPeriod());
    }

    @Deprecated
    public static List<Actuator> getActuators(Properties properties) throws Exception {
        List<Actuator> actuators = new LinkedList<>();

        Properties actuatorsProperties = properties.getSubset(PARAM);

        // TODO backward compatibility
        Properties sinkPropertiesOld = properties.getSubset("notifications.sink");
        actuatorsProperties.putAll(sinkPropertiesOld);
        // TODO backward compatibility

        Set<String> ids = actuatorsProperties.getIDs();

        for (String id : ids) {
            Properties props = actuatorsProperties.getSubset(id);

            Actuator sink = ComponentManager.build(Type.ACTUATOR, props);
            sink.setId(id);

            actuators.add(sink);
        }

        return actuators;
    }

    public static void run(JavaDStream<Action> actions, Properties propertiesSourceProps) {
        actions.foreachRDD(rdd -> rdd.foreach(new RunActuatorsF(propertiesSourceProps)));
    }

}
