package ch.cern.spark.metrics.trigger.action.actuator;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.trigger.action.Action;
import lombok.Getter;
import lombok.Setter;

@ComponentType(Type.ACTUATOR)
public abstract class Actuator extends Component {

    private static final long serialVersionUID = 8984201586179047078L;
    
    @Getter @Setter
    private String id;

	protected boolean shouldBeProcess(Action action) {
        return action.getActuatorIDs().contains(id) || action.getActuatorIDs().contains("ALL");
    }

    protected abstract void run(Action action) throws Exception;
	
}
