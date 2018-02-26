package ch.cern.spark.metrics.trigger.action.actuator;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.trigger.action.Action;
import lombok.Getter;
import lombok.Setter;

@ComponentType(Type.ACTUATOR)
public abstract class Actuator extends Component {

    private static final long serialVersionUID = 8984201586179047078L;
    
    private transient final static Logger LOG = Logger.getLogger(Actuator.class.getName());
    
    @Getter @Setter
    private String id;

	protected boolean shouldBeProcess(Action action) {
        return action.getActuatorIDs().contains(id) || action.getActuatorIDs().contains("ALL");
    }
	
    public void sink(JavaDStream<Action> actions) {
        actions
            .filter(action -> shouldBeProcess(action))
            .foreachRDD(rdd -> rdd.foreachAsync(action -> {
                    try {
                        run(action);
                    }catch(Exception e) {
                        LOG.error("Error when running action=" + action, e);
                    }
                }));
    }   

    protected abstract void run(Action action) throws Exception;
	
}
