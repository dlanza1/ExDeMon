package ch.cern.exdemon.metrics.defined;

import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.State;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.properties.Properties;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateDefinedMetricStatusesF extends UpdateStatusFunction<DefinedMetricStatuskey, Metric, VariableStatuses, Metric>{

	private static final long serialVersionUID = 2965182980222300453L;
	
	private final static Logger LOG = Logger.getLogger(UpdateDefinedMetricStatusesF.class.getName());

	private Properties propertiesSourceProps;

	public UpdateDefinedMetricStatusesF(Properties propertiesSourceProps) {
		this.propertiesSourceProps = propertiesSourceProps;
	}
	
    @Override
    protected Optional<Metric> update(DefinedMetricStatuskey id, Metric metric, State<VariableStatuses> status) 
            throws Exception {
        Optional<DefinedMetric> definedMetricOpt = getDefinedMetric(id.getID());
        if(!definedMetricOpt.isPresent()) {
            status.remove();
            return Optional.empty();
        }
        DefinedMetric definedMetric = definedMetricOpt.get();
            
        VariableStatuses varStatuses = getStatus(status);

        Optional<Metric> newMetric = Optional.empty();
        try {
            definedMetric.updateStore(varStatuses, metric, id.getMetric_attributes().keySet());
            
            newMetric = definedMetric.generateByUpdate(varStatuses, metric, id.getMetric_attributes());
        }catch(Exception e) {
            LOG.error("ID:" + id
                    + " Metric: " + metric
                    + " VariableStatuses: " + varStatuses
                    + " Message:" + e.getMessage(), e);
            
            newMetric = Optional.of(new Metric(
                                            metric.getTimestamp(), 
                                            new ExceptionValue("Error when processing defined metric: " + e.getMessage()), 
                                            id.getMetric_attributes()));   
        }
        
        status.update(varStatuses);
        
        return newMetric;
    }

	protected Optional<DefinedMetric> getDefinedMetric(String id) throws Exception {
	    ComponentsCatalog.init(propertiesSourceProps);
	    
        return ComponentsCatalog.get(Type.METRIC, id);
    }

    private VariableStatuses getStatus(State<VariableStatuses> status) {
		return status.exists() ? status.get() : new VariableStatuses();
	}

}
