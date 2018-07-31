package ch.cern.exdemon.metrics.defined.equation;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.defined.equation.var.Variable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
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
	
	public Map<String, ValueVariable> getMetricVariables() {
		HashMap<String, ValueVariable> metricVariables = new HashMap<>();
		
		for (Map.Entry<String, Variable> variable : getVariables().entrySet())
			if(variable.getValue() instanceof ValueVariable)
				metricVariables.put(variable.getKey(), (ValueVariable) variable.getValue());
		
		return metricVariables;
	}

	@Override
	public Class<Value> returnType() {
		return Value.class;
	}

}
