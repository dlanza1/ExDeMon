package ch.cern.spark.metrics.defined.equation;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.var.MetricVariable;
import ch.cern.spark.metrics.defined.equation.var.Variable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.value.Value;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class Equation implements ValueComputable{
	
	private static EquationParser parser = new EquationParser();

	private ValueComputable formula;
	
	@Getter
	private Map<String, Variable> variables = new HashMap<>();

	public Equation(String equationString, Properties variablesProperties) throws ParseException, ConfigurationException {
		this.formula = parser.parse(equationString, variablesProperties, variables);
	}
	
	@Override
	public Value compute(VariableStatuses stores, Instant time) {
		return formula.compute(stores, time);
	}
	
	public Map<String, MetricVariable> getMetricVariables() {
		HashMap<String, MetricVariable> metricVariables = new HashMap<>();
		
		for (Map.Entry<String, Variable> variable : getVariables().entrySet())
			if(variable.getValue() instanceof MetricVariable)
				metricVariables.put(variable.getKey(), (MetricVariable) variable.getValue());
		
		return metricVariables;
	}

	@Override
	public Class<Value> returnType() {
		return Value.class;
	}

}
