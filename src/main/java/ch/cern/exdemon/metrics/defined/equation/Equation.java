package ch.cern.exdemon.metrics.defined.equation;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

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

	@Override
	public Class<Value> returnType() {
		return Value.class;
	}

}
