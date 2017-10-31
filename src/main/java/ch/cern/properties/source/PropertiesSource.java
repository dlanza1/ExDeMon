package ch.cern.properties.source;

import ch.cern.components.Component;
import ch.cern.properties.Properties;

public abstract class PropertiesSource extends Component {

	private static final long serialVersionUID = 4436444683021922084L;
	
	public static String CONFIGURATION_PREFIX = "properties.source";

	public PropertiesSource() {
		super(Component.Type.PROPERTIES_SOURCE);
	}
	
	public PropertiesSource(Class<? extends Component> subClass, String name) {
		super(Component.Type.PROPERTIES_SOURCE, subClass, name);
	}
	
	public abstract Properties load() throws Exception;

}
