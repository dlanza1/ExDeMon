package ch.cern;

public class PropertiesSourceTest extends PropertiesSource {

	private static final long serialVersionUID = 79323351398301182L;
	
	@Override
	public Properties load() {
		Properties properties = new Properties();
		
		properties.setProperty("metrics.source.type", "not-valid-already-declared");
		properties.setProperty("results.sink.type", "not-valid-already-declared");
		
		properties.setProperty("key1", "val1");
		properties.setProperty("key2", "val2");
		
		return properties;
	}

}
