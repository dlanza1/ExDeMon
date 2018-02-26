package ch.cern.spark.metrics.trigger.action.actuator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.trigger.action.Action;

public class RunActuatorsF implements VoidFunction<Action> {

    private static final long serialVersionUID = -7248445923365556277L;
    
    private transient final static Logger LOG = Logger.getLogger(RunActuatorsF.class.getName());
    
    private Properties propertiesSourceProps;

    public RunActuatorsF(Properties propertiesSourceProps) {
        this.propertiesSourceProps = propertiesSourceProps;
    }

    @Override
    public void call(Action action) throws Exception {
        Actuators.initCache(propertiesSourceProps);
        
        action.getActuatorIDs().stream().forEach(actuatorID -> {
            try {
                Actuator actuator = Actuators.getCache().get().get(actuatorID);
                
                if(actuator != null)
                    actuator.run(action);
                else
                    LOG.error("Action " + action + " could not be run because actuator with id " + actuatorID + " does not exist.");
            } catch (Exception e) {
                LOG.error("Actuator ID=" +actuatorID + ": problem when running action=" + action, e);
            }
        });
    }

}
