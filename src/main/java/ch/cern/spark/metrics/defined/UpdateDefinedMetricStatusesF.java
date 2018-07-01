package ch.cern.spark.metrics.defined;

import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.State;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentsCatalog;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.value.ExceptionValue;
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
            
        VariableStatuses store = getStore(status);

        Optional<Metric> newMetric = Optional.empty();
        try {
            definedMetric.updateStore(store, metric, id.getMetric_attributes().keySet());
            
            newMetric = definedMetric.generateByUpdate(store, metric, id.getMetric_attributes());
        }catch(Exception e) {
            LOG.error("ID:" + id
                    + " Metric: " + metric
                    + " VariableStatuses: " + store
                    + " Message:" + e.getMessage(), e);
            
            newMetric = Optional.of(new Metric(
                                            metric.getTimestamp(), 
                                            new ExceptionValue("Error when processing defined metric: " + e.getMessage()), 
                                            id.getMetric_attributes()));   
        }
        
        status.update(store);
        
        return newMetric;
    }

	protected Optional<DefinedMetric> getDefinedMetric(String id) throws Exception {
	    ComponentsCatalog.init(propertiesSourceProps);
	    
        return ComponentsCatalog.get(Type.METRIC, id);
    }

    private VariableStatuses getStore(State<VariableStatuses> status) {
		return status.exists() ? status.get() : new VariableStatuses();
	}

}
