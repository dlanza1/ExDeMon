package ch.cern.properties.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.schema.MetricSchemas;
import ch.cern.spark.metrics.trigger.action.actuator.Actuators;

@ComponentType(Type.PROPERTIES_SOURCE)
public abstract class PropertiesSource extends Component {

	private static final long serialVersionUID = 4436444683021922084L;

	public static String CONFIGURATION_PREFIX = "properties.source";
	
	public List<Pattern> id_filters = new LinkedList<>();

    private Properties staticProperties;

	@Override
	public final void config(Properties properties) throws ConfigurationException {
	    Properties filtersProps = properties.getSubset("id.filters");
	    if(filtersProps.size() > 0) {
	        id_filters = new LinkedList<>();
	        for (String id : filtersProps.getIDs())
	            id_filters.add(Pattern.compile(filtersProps.getProperty(id)));
	    }else{
	        id_filters = Arrays.asList(Properties.ID_REGEX);
	    }
	    
	    staticProperties = properties.getSubset("static");
	    
	    configure(properties);
	}
	
	protected void configure(Properties properties) throws ConfigurationException {
	}

	public Properties load() throws Exception{
	    Properties props = loadAll();
	    props.setStaticProperties(staticProperties);
	    
	    props.entrySet().removeIf(entry -> {
               String key = (String) entry.getKey();
               
               if(key.startsWith(MetricSchemas.PARAM))
                   key = key.replaceFirst(MetricSchemas.PARAM + ".", "");
               else if (key.startsWith(Monitors.PARAM))
                   key = key.replaceFirst(Monitors.PARAM + ".", "");
               else if (key.startsWith(DefinedMetrics.PARAM))
                   key = key.replaceFirst(DefinedMetrics.PARAM + ".", "");
               else if (key.startsWith(Actuators.PARAM))
                   key = key.replaceFirst(Actuators.PARAM + ".", "");
               else
                   return true;
               
               String id = key.substring(0, key.indexOf('.'));
               
               for (Pattern id_filter : id_filters)
                   if(id_filter.matcher(id).matches())
                       return false;
               
               return true;
           });
	    
	    return props;
	}
	
	protected abstract Properties loadAll() throws Exception;

}
