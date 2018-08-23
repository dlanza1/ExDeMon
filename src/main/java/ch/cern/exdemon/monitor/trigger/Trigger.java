package ch.cern.exdemon.monitor.trigger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import lombok.Getter;
import lombok.ToString;

@ToString
@ComponentType(Type.TRIGGER)
public abstract class Trigger extends Component implements Function<AnalysisResult, Optional<Action>>, Serializable {

    private static final long serialVersionUID = -5418973482734557441L;

    protected Set<String> actuatorIDs;

    protected Map<String, String> tags;

    protected MetricsFilter filter;

    private static final String SILENT_PARAM = "silent";
    private static final String SILENT_PERIOD_PARAM = SILENT_PARAM + ".period";
    private static final String SILENT_TRIGGER_PARAM = SILENT_PARAM + ".trigger";

    @Getter
    private Duration silentPeriod;

    @Getter
    protected Trigger silentPeriodTrigger;

    public Trigger() {
    }

    @Override
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        String actuatorIDsString = properties.getProperty("actuators");

        if (actuatorIDsString != null)
            actuatorIDs = new HashSet<>(Arrays.asList(actuatorIDsString.split("\\s")));
        else
            actuatorIDs = new HashSet<>();

        tags = properties.getSubset("tags").toStringMap();

        filter = new MetricsFilter();
        confResult.merge("filter", filter.config(properties.getSubset("filter")));

        try {
            silentPeriod = properties.getPeriod(SILENT_PERIOD_PARAM, Duration.ofSeconds(0));
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }

        Properties silentPeriodTriggerProps = properties.getSubset(SILENT_TRIGGER_PARAM);

        if (silentPeriodTriggerProps.isTypeDefined()) {
            ComponentBuildResult<Trigger> silentPeriodTriggerBuildResult = ComponentTypes.build(Type.TRIGGER, "silent-period", silentPeriodTriggerProps);
            confResult.merge(SILENT_TRIGGER_PARAM, silentPeriodTriggerBuildResult.getConfigurationResult());
            
            silentPeriodTrigger = silentPeriodTriggerBuildResult.getComponent().get();
        }
        
        return confResult;
    }

    public Optional<Action> apply(AnalysisResult result) {
        if (!filter.test(result.getAnalyzed_metric()))
            return Optional.empty();

        Optional<String> reasonOpt = process(result.getStatus(), result.getAnalyzed_metric().getTimestamp());

        if (!reasonOpt.isPresent())
            return Optional.empty();

        HashMap<String, String> triggerTags = new HashMap<>(result.getTags());
        triggerTags.putAll(tags);

        return Optional.of(new Action("", getId(), result.getAnalyzed_metric().getAttributes(), reasonOpt.get(),
                actuatorIDs, triggerTags, result));
    }

    public abstract Optional<String> process(Status status, Instant timestamp);

}
