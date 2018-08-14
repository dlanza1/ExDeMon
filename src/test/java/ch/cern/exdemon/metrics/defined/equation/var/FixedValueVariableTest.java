package ch.cern.exdemon.metrics.defined.equation.var;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.DefinedMetric;
import ch.cern.exdemon.metrics.defined.DefinedMetricTest;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class FixedValueVariableTest {

    @Test
    public void fixedValueVariableIsTakenIfMissingStatuses() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = DefinedMetricTest.newProperties();
        properties.setProperty("metrics.attribute.attA.variable", "varA");
        properties.setProperty("when", "ANY");
        properties.setProperty("value", "value");
        properties.setProperty("variables.value.filter.attribute.VAR", "value");
        properties.setProperty("variables.varA.filter.attribute.VARA", "value");
        properties.setProperty("variables.varA.attribute", "VARA");
        properties.setProperty("variables.varA.merge.variables", "varB");
        properties.setProperty("variables.varB.filter.attribute.VARB", "value");
        properties.setProperty("variables.varB.fixed.value", "fixedValue");
        definedMetric.config(properties);

        VariableStatuses varStatuses = new VariableStatuses();

        Metric metric = Metric(1, "anyValue", "VAR=value");
        definedMetric.updateStore(varStatuses, metric, new HashSet<>());
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(varStatuses, metric, new HashMap<String, String>());
        assertEquals("fixedValue", generatedMetric.get().getAttributes().get("attA"));
        
        metric = Metric(1, "anyValue", "VARA=value");
        definedMetric.updateStore(varStatuses, metric, new HashSet<>());
        generatedMetric = definedMetric.generateByUpdate(varStatuses, metric, new HashMap<String, String>());
        assertEquals("value", generatedMetric.get().getAttributes().get("attA"));
    }

}
