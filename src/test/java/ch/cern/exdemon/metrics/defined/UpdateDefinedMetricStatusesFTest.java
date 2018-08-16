package ch.cern.exdemon.metrics.defined;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.Properties;

public class UpdateDefinedMetricStatusesFTest {

    @Test
    public void shouldGenerateWhenUpdatingVariable() throws Exception {
        Properties dmProps = DefinedMetricTest.newProperties();
        dmProps.setProperty("metrics.groupby", "DB_NAME, METRIC_NAME");
        dmProps.setProperty("variables.value.aggregate.type", "count_floats");
        dmProps.setProperty("variables.value.aggregate.attributes", "ALL");
        dmProps.setProperty("variables.value.expire", "10s");
        DefinedMetric dm = new DefinedMetric("dmID1");
        dm.config(dmProps);
        
        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesFWithDefinedMetric(dm);

        DefinedMetricStatuskey id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
        State<VariableStatuses> status = new StateImpl<>();
        Metric metric = null;
        
        metric = Metric(0, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_1", "METRIC_NAME=Read");
        Optional<Metric> result = func.update(id, metric, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(0, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_2", "METRIC_NAME=Read");
        result = func.update(id, metric, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(10, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_1", "METRIC_NAME=Read");
        result = func.update(id, metric, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(10, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_2", "METRIC_NAME=Read");
        result = func.update(id, metric, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);
    }
    
    @Test
    public void shouldAggregateAlongTime() throws Exception {
        Properties dmProps = DefinedMetricTest.newProperties();
    	dmProps.setProperty("metrics.groupby", "INSTANCE_NAME");
    	dmProps.setProperty("variables.value.aggregate.type", "count_floats");
    	dmProps.setProperty("variables.value.expire", "5s");
        DefinedMetric dm = new DefinedMetric("dmID1");
        dm.config(dmProps);
        
        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesFWithDefinedMetric(dm);

        Map<String, String> groupByIDs = new HashMap<>();
        groupByIDs.put("INSTANCE_NAME", "DB1_1");
        DefinedMetricStatuskey id = new DefinedMetricStatuskey("dmID1", groupByIDs);
        State<VariableStatuses> status = new StateImpl<>();
        VariableStatuses varStores = new VariableStatuses();
		ValueHistory valueHistory = new ValueHistory(100, 0, null, null);
        varStores.put("value", new ValueVariable.Status_(valueHistory ));
		status.update(varStores);
        Metric metric = null;
        
        metric = Metric(0, 0f, "INSTANCE_NAME=DB1_1");
        Optional<Metric> result = func.update(id, metric, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(1, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(2, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
    }

    @Test
    public void shouldExpireValuesWhenGroupByIncludeAllAttributes() throws Exception {
        Properties dmProps = DefinedMetricTest.newProperties();
        dmProps.setProperty("metrics.groupby", "INSTANCE_NAME");
        dmProps.setProperty("variables.value.aggregate.type", "count_floats");
        dmProps.setProperty("variables.value.expire", "5s");
    	DefinedMetric dm = new DefinedMetric("dmID1");
        dm.config(dmProps);
        
        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesFWithDefinedMetric(dm);

        Map<String, String> groupByIDs = new HashMap<>();
        groupByIDs.put("INSTANCE_NAME", "DB1_1");
        DefinedMetricStatuskey id = new DefinedMetricStatuskey("dmID1", groupByIDs);
        State<VariableStatuses> status = new StateImpl<>();
        Metric metric= null;
        
        metric = Metric(0, 0f, "INSTANCE_NAME=DB1_1");
        Optional<Metric> result = func.update(id, metric, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(2, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(4, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
        
        //Metric at time 0 expired
        metric = Metric(6, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
        
        //Metric at time 1 expired
        metric = Metric(8, 0f, "INSTANCE_NAME=DB1_1");
        result = func.update(id, metric, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
    }
    
    public static class UpdateDefinedMetricStatusesFWithDefinedMetric extends UpdateDefinedMetricStatusesF {

        private static final long serialVersionUID = 1L;
        private Optional<DefinedMetric> df;
        
        public UpdateDefinedMetricStatusesFWithDefinedMetric(DefinedMetric definedMetric) {
            super(null);
            
            this.df = Optional.ofNullable(definedMetric);
        }
        
        @Override
        protected Optional<DefinedMetric> getDefinedMetric(String id) throws Exception {
            return df;
        }
        
    }
    
}
