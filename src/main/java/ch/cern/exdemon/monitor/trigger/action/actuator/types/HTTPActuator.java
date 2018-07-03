package ch.cern.exdemon.monitor.trigger.action.actuator.types;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.http.HTTPSink;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.actuator.Actuator;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import lombok.ToString;

@RegisterComponentType("http")
@ToString(callSuper=false)
public class HTTPActuator extends Actuator {

	private static final long serialVersionUID = 6368509840922047167L;

	private HTTPSink sink = new HTTPSink();

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		sink.config(properties);
	}

	@Override
	protected void run(Action action) throws Exception {    
		sink.sink(action);	
	}

}
