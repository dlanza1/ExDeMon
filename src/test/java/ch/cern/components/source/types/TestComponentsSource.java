package ch.cern.components.source.types;

import ch.cern.components.RegisterComponentType;
import ch.cern.components.source.ComponentsSource;
import ch.cern.properties.Properties;

@RegisterComponentType("test")
public class TestComponentsSource extends ComponentsSource {

    private static final long serialVersionUID = -5721933057258619601L;

    public void add(Type componentType, String id, Properties properties) {
        register(componentType, id, properties);
    }
    
}
