package ch.cern.spark.metrics.notifications.sink.types;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

@RegisterComponent("influx")
public class InfluxNotificationsSink extends NotificationsSink {
    
	private static final long serialVersionUID = -3201411248735120722L;

	@Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);

        
    }

    @Override
    public void sink(Stream<Notification> outputStream) {
        JavaDStream<String> jsonStringsStream = outputStream.asJSON().asString().asJavaDStream();
        
        
    }

}
