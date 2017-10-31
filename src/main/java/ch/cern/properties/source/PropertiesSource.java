package ch.cern.properties.source;

import ch.cern.components.Component;
import ch.cern.components.ComponentType;
import ch.cern.components.Component.Type;
import ch.cern.properties.Properties;

@ComponentType(Type.PROPERTIES_SOURCE)
public abstract class PropertiesSource extends Component {

	private static final long serialVersionUID = 4436444683021922084L;
	
	public static String CONFIGURATION_PREFIX = "properties.source";

	public abstract Properties load() throws Exception;

}
