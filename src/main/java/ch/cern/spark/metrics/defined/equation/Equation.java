package ch.cern.spark.metrics.defined.equation;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.var.MetricVariable;
import ch.cern.spark.metrics.defined.equation.var.Variable;
import ch.cern.spark.metrics.defined.equation.var.VariableStores;
import ch.cern.spark.metrics.value.Value;

public class Equation implements ValueComputable{
	
	private static EquationParser parser = new EquationParser();

	private ValueComputable formula;
	
	private Map<String, Variable> variables = new HashMap<>();

	public Equation(String equationString, Properties variablesProperties) throws ParseException, ConfigurationException {
		this.formula = parser.parse(equationString, variablesProperties, variables);
	}
	
	@Override
	public Value compute(VariableStores stores, Instant time) {
		return formula.compute(stores, time);
	}

	@Override
	public String toString() {
		return formula.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((formula == null) ? 0 : formula.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Equation other = (Equation) obj;
		if (formula == null) {
			if (other.formula != null)
				return false;
		} else if (!formula.equals(other.formula))
			return false;
		return true;
	}

	public Map<String, Variable> getVariables() {
		return variables;
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
