package ch.cern.exdemon.monitor.trigger.action.actuator;

import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class RunActuatorsF implements VoidFunction<Action> {

    private static final long serialVersionUID = -7248445923365556277L;
    
    private transient final static Logger LOG = Logger.getLogger(RunActuatorsF.class.getName());
    
    private Properties componentsSourceProperties;

    public RunActuatorsF(Properties componentsSourceProperties) {
        this.componentsSourceProperties = componentsSourceProperties;
    }

    @Override
    public void call(Action action) throws Exception {
        ComponentsCatalog.init(componentsSourceProperties);
        
        action.getActuatorIDs().stream().forEach(actuatorID -> {
            try {
                Optional<Actuator> actuatorOpt = ComponentsCatalog.get(Type.ACTUATOR, actuatorID);
                
                if(actuatorOpt.isPresent())
                    actuatorOpt.get().run(action);
                else
                    LOG.error("Action " + action + " could not be run because actuator with id " + actuatorID + " does not exist.");
            } catch (Exception e) {
                LOG.error("Actuator ID=" +actuatorID + ": problem when running action=" + action, e);
            }
        });
    }

}
