package ch.cern.spark.metrics.trigger.action.actuator;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.trigger.action.Action;

@ComponentType(Type.ACTUATOR)
public abstract class Actuator extends Component implements Sink<Action>{

    private static final long serialVersionUID = 8984201586179047078L;
    
    private String id;

    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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
