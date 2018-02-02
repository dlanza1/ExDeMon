package ch.cern.spark.metrics.notificator.types;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import lombok.ToString;

@ToString
@RegisterComponent("statuses")
public class StatusesNotificator extends Notificator {
    
    private static final long serialVersionUID = -7890231998987060652L;

    private static final String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;

    @Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
        expectedStatuses = Stream.of(properties.getProperty(STATUSES_PARAM).split("\\s"))
					        		.map(String::trim)
					        		.map(String::toUpperCase)
					        		.map(Status::valueOf)
					        		.collect(Collectors.toSet());
        
        properties.confirmAllPropertiesUsed();
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
