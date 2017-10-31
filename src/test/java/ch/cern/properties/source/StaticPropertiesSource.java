package ch.cern.properties.source;

import ch.cern.properties.Properties;

public class StaticPropertiesSource extends PropertiesSource{

	private static final long serialVersionUID = -7121294339770042193L;
	
	public static Properties properties;

	@Override
	public Properties load() throws Exception {
		return properties;
	}
	
}
