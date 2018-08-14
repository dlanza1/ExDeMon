package ch.cern.exdemon.components.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentRegistrationResult;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.properties.Properties;

@ComponentType(Type.COMPONENTS_SOURCE)
public abstract class ComponentsSource extends Component {

    private static final long serialVersionUID = -7276957377600776486L;
    
    private final static Logger LOG = Logger.getLogger(ComponentsSource.class.getName());

    public static String PARAM = "components.source";
    
    private List<Pattern> id_filters = new LinkedList<>();

    private Properties staticProperties;
    
    public ComponentsSource() {
    }
    
    @Override
    public final ConfigurationResult config(Properties properties) {
        Properties filtersProps = properties.getSubset("id.filters");
        if(filtersProps.size() > 0) {
            id_filters = new LinkedList<>();
            for (String id : filtersProps.getIDs())
                id_filters.add(Pattern.compile(filtersProps.getProperty(id)));
        }else{
            id_filters = Arrays.asList(Properties.ID_REGEX);
        }
        
        staticProperties = properties.getSubset("static");
        
        return configure(properties);
    }
    
    protected ConfigurationResult configure(Properties properties) {
        return ConfigurationResult.SUCCESSFUL();
    }

    public final Optional<ComponentRegistrationResult> register(Type componentType, String id, Properties properties) {
        boolean filter = filterID(id);
        if(!filter)
            return Optional.empty();
        
        properties.setStaticProperties(staticProperties);
        
        ComponentRegistrationResult componentRegistrationResult = ComponentsCatalog.register(componentType, id, properties);
        
        pushComponentRegistrationResult(componentRegistrationResult);
        
        return Optional.of(componentRegistrationResult);
    }

    private boolean filterID(String id) {
        if(id_filters == null || id_filters.isEmpty())
            return true;
        
        for (Pattern id_filter : id_filters)
            if(id_filter.matcher(id).matches())
                return true;
        
        return false;
    }

    protected final void remove(Type componentType, String id) {
        ComponentsCatalog.remove(componentType, id);
    }
    
    protected void pushComponentRegistrationResult(ComponentRegistrationResult componentRegistration) {
        switch(componentRegistration.getStatus()) {
        case OK:
            LOG.info(componentRegistration);
            break;
        case WARNING:
            LOG.warn(componentRegistration);
            break;
        case ERROR:
            LOG.error(componentRegistration);
            break;
        case EXISTING:
            LOG.debug(componentRegistration);
            break;
        }
    }

    public void initialize() throws Exception {
    }

    public void close() {
    }

    public void addToReport(Type componentType, String componentId, String reportName, String content) {
        LOG.info("Report for " + componentType + " with id=" + componentId + " for report=" + reportName + ": " + content);
    }

}
