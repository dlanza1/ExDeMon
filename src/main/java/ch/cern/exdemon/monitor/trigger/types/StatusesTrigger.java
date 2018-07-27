package ch.cern.exdemon.monitor.trigger.types;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.trigger.Trigger;
import ch.cern.properties.Properties;
import lombok.ToString;

@ToString
@RegisterComponentType("statuses")
public class StatusesTrigger extends Trigger {
    
    private static final long serialVersionUID = -7890231998987060652L;

    private static final String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;

    @Override
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult configResult = super.config(properties);
        
        expectedStatuses = Stream.of(properties.getProperty(STATUSES_PARAM).split("\\s"))
					        		.map(String::trim)
					        		.map(String::toUpperCase)
					        		.map(Status::valueOf)
					        		.collect(Collectors.toSet());
        
        return configResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }

    @Override
    public Optional<String> process(Status status, Instant timestamp) {
        if(isExpectedStatus(status)){
            return Optional.of("Metric is in status " + status + ".");
        }else{
            return Optional.empty();
        }
    }
    
    private boolean isExpectedStatus(Status status) {
        return expectedStatuses.contains(status);
    }

}
