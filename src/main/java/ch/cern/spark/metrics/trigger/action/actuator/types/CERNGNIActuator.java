package ch.cern.spark.metrics.trigger.action.actuator.types;

import ch.cern.components.RegisterComponent;
import ch.cern.monitoring.gni.GNINotification;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.metrics.trigger.action.actuator.Actuator;
import lombok.ToString;

@RegisterComponent("cern-gni")
@ToString(callSuper=false)
public class CERNGNIActuator extends Actuator {

	private static final long serialVersionUID = 6416955181811280312L;
	
	private Properties contentProperties;
	
	private HTTPSink sink = new HTTPSink();

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		sink.config(properties);
		
		contentProperties = properties.getSubset("content");
	}
	
	@Override
	protected void run(Action action) throws Exception {
	    GNINotification notif = GNINotification.from(contentProperties, action);

        sink.sink(notif);
	}

}
