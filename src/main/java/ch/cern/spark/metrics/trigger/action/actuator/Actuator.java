package ch.cern.spark.metrics.trigger.action.actuator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.metrics.trigger.action.actuator.types.EmailActuator;
import lombok.Getter;
import lombok.Setter;

@ComponentType(Type.ACTUATOR)
public abstract class Actuator extends Component implements Sink<Action>{

    private static final long serialVersionUID = 8984201586179047078L;
    
    private final static Logger LOG = LogManager.getLogger(EmailActuator.class);
    
    @Getter @Setter
    private String id;

    @Override
    public final void config(Properties properties) throws ConfigurationException {
        try {
            tryConfig(properties);
        }catch(Exception e) {
            LOG.error("ID: " + id + ": " + e.getMessage(), e);
        }
    }

    protected void tryConfig(Properties properties) throws ConfigurationException {
    }

    @Override
	public final void sink(JavaDStream<Action> actions) {
		run(actions.filter(action -> shouldBeProcess(action)));
	}

	protected boolean shouldBeProcess(Action action) {
        return action.getActuatorIDs().contains(id) || action.getActuatorIDs().contains("ALL");
    }

    protected abstract void run(JavaDStream<Action> notifications);	
	
}
