package ch.cern.properties.source;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.Properties;

@RegisterComponent("static")
public class StaticPropertiesSource extends PropertiesSource{

	private static final long serialVersionUID = -7121294339770042193L;
	
	public static Properties properties = new Properties();

	@Override
	public Properties loadAll() throws Exception {
		return properties;
	}
	
}
