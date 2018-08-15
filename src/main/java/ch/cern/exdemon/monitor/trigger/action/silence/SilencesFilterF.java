package ch.cern.exdemon.monitor.trigger.action.silence;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.api.java.function.Function;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class SilencesFilterF implements Function<Action, Boolean> {

    private static final long serialVersionUID = 7712606037271217706L;

    private Properties componentsSourceProperties;

    public SilencesFilterF(Properties componentsSourceProps) {
        this.componentsSourceProperties = componentsSourceProps;
    }

    @Override
    public Boolean call(Action action) throws Exception {
        ComponentsCatalog.init(componentsSourceProperties);
        
        Map<String, Silence> silences = ComponentsCatalog.get(Type.SILENCE);
        
        Optional<Boolean> filterResult = silences.values().stream()
                                                          .filter(silence -> silence.isActiveAt(action.getCreation_timestamp()))
                                                          .map(silence -> shouldSilentAndReport(action, silence))
                                                          .reduce((a, b) -> a && b);
        
        return filterResult.orElse(true);
    }

    private boolean shouldSilentAndReport(Action action, Silence silence) {
        boolean actionAllowed = silence.filter(action);
        
        if(!actionAllowed)
            ComponentsCatalog.addToReport(Type.SILENCE, 
                                 silence.getId(),
                                 "silenced",
                                 JSONParser.parse(action).toString());
        
        return actionAllowed;
    }

}
