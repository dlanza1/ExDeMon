package ch.cern.spark.metrics.defined.equation;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.spark.metrics.defined.equation.functions.bool.AndFunc;
import ch.cern.spark.metrics.defined.equation.functions.bool.EqualFunc;
import ch.cern.spark.metrics.defined.equation.functions.bool.IfBoolFunc;
import ch.cern.spark.metrics.defined.equation.functions.bool.NotEqualFunc;
import ch.cern.spark.metrics.defined.equation.functions.bool.NotFunc;
import ch.cern.spark.metrics.defined.equation.functions.bool.OrFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.AbsFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.AddFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.CosFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.DivFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.GTFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.IfFloatFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.LTFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.MinusFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.MulFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.PowFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.SinFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.SqrtFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.SubFunc;
import ch.cern.spark.metrics.defined.equation.functions.num.TanFunc;
import ch.cern.spark.metrics.defined.equation.functions.string.ConcatFunc;
import ch.cern.spark.metrics.defined.equation.functions.string.IfStringFunc;
import ch.cern.spark.metrics.defined.equation.functions.string.TrimFunc;
import ch.cern.spark.metrics.defined.equation.var.AnyMetricVariable;
import ch.cern.spark.metrics.defined.equation.var.BooleanMetricVariable;
import ch.cern.spark.metrics.defined.equation.var.FloatMetricVariable;
import ch.cern.spark.metrics.defined.equation.var.MetricVariable;
import ch.cern.spark.metrics.defined.equation.var.StringMetricVariable;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class EquationParser {
	
	private int pos = -1, ch;
	
	private String str;

	private Properties variablesProperties;

	private Map<String, MetricVariable> variables;

	private Set<String> variableNames;
	
	public EquationParser() {
	}

	public synchronized ValueComputable parse(
			String equationAsString, 
			Properties variablesProperties, 
			Map<String, MetricVariable> variables) throws ParseException, ConfigurationException {
		
		this.variablesProperties = variablesProperties;
		this.variableNames = variablesProperties.getUniqueKeyFields();
		this.variables = variables;
		str = equationAsString;
		
		pos = -1;
		ch = 0;
		
        nextChar();
        
        ValueComputable x = parseExpression();
        
        if (pos < str.length()) 
        		throw new ParseException("Unexpected: " + (char)ch, pos);
        
        return x;
    }
	
	private void nextChar() {
        ch = (++pos < str.length()) ? str.charAt(pos) : -1;
    }

	private boolean eat(String opToEat) {
		while (ch == ' ') nextChar();
		if (str.substring(pos).startsWith(opToEat)) {
			for (int i = 0; i < opToEat.length(); i++)
            		nextChar();
            return true;
        }
        return false;
	}

	private ValueComputable parseExpression() throws ParseException, ConfigurationException {
		ValueComputable x = parseTerm();
		
		for (;;) {
	        if      (eat(AddFunc.REPRESENTATION)) 	
	        		x = new AddFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseTerm(), FloatMetricVariable.class));
	        else if (eat(SubFunc.REPRESENTATION)) 	
	        		x = new SubFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseTerm(), FloatMetricVariable.class));
	        else return x;
	    }
    }

	private ValueComputable parseTerm() throws ParseException, ConfigurationException {
		ValueComputable x = parseBooleanOperation();
		
		for (;;) {
	        if      (eat(MulFunc.REPRESENTATION)) 	
	        		x = new MulFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseFactor(), FloatMetricVariable.class));
	        else if (eat(DivFunc.REPRESENTATION)) 	
	        		x = new DivFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseFactor(), FloatMetricVariable.class));
	        else if (eat(PowFunc.REPRESENTATION)) 	
	        		x = new PowFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseFactor(), FloatMetricVariable.class));
	        else return x;
	    }
    }
	
	private ValueComputable parseBooleanOperation() throws ParseException, ConfigurationException {
		ValueComputable x = parseFactor();
		
		for (;;) {
	        if      (eat(AndFunc.REPRESENTATION)) 	
	        		x = new AndFunc(typeVariable(x, BooleanMetricVariable.class), typeVariable(parseFactor(), BooleanMetricVariable.class));
	        else if (eat(OrFunc.REPRESENTATION)) 	
	        		x = new OrFunc(typeVariable(x, BooleanMetricVariable.class), typeVariable(parseFactor(), BooleanMetricVariable.class));
	        else if (eat(GTFunc.REPRESENTATION)) 	
        			x = new GTFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseFactor(), FloatMetricVariable.class));
	        else if (eat(LTFunc.REPRESENTATION)) 	
        			x = new LTFunc(typeVariable(x, FloatMetricVariable.class), typeVariable(parseFactor(), FloatMetricVariable.class));
	        else if (eat(EqualFunc.REPRESENTATION)) 	
	        		x = new EqualFunc(x, parseFactor());
	        else if (eat(NotEqualFunc.REPRESENTATION)) 	
	        		x = new NotEqualFunc(x, parseFactor());
	        else return x;
	    }
	}

	private ValueComputable parseFactor() 
			throws ParseException, ConfigurationException {
		
        if (eat("+")) 
        		return parseFactor(); // unary plus
        if (eat(MinusFunc.REPRESENTATION)) 
        		return new MinusFunc(parseFactor()); // unary minus
        if (eat(NotFunc.REPRESENTATION)) 
    			return new NotFunc(typeVariable(parseFactor(), BooleanMetricVariable.class)); // negate

        ValueComputable x;
        int startPos = this.pos;
        if (eat("(")) { // parentheses
            x = parseExpression();
            eat(")");
        } else if (Character.isDigit(ch) || ch == '.') { // numbers
            while (Character.isDigit(ch) || ch == '.') nextChar();
            x = FloatValue.from(str.substring(startPos, this.pos));
        } else if (ch == '"') { //strings
        		x = parseStringLiteral(startPos, ch);
        		eat("\"");
        } else if (ch == '\'') { //strings
	    		x = parseStringLiteral(startPos, ch);
	    		eat("'");
        } else if (Character.isLetter(ch)) { // functions, boolean and variables
            while (Character.isLetterOrDigit(ch) || ch == '_') nextChar();
            String text = str.substring(startPos, this.pos);
            
            if(ch == '(')
            		x = parseFunction(text);
            else if(text.equals("true") || text.equals("false"))
            		x = BooleanValue.from(text);
            else
            		x = parseVariable(text, AnyMetricVariable.class);
        } else {
            throw new ParseException("Unexpected: " + (char)ch, pos);
        }

        return x;
    }

	private ValueComputable parseStringLiteral(int startPos, int delimiterChar) {
		nextChar();
		while (ch != delimiterChar) {
			if(ch == '\\')
				nextChar();
			nextChar();
		}
		String text = str.substring(startPos + 1, this.pos);

		return new StringValue(text);
	}

	private ValueComputable typeVariable(ValueComputable possibleMetricVariable, Class<? extends MetricVariable> toType) 
			throws ConfigurationException, ParseException {

		if(!(possibleMetricVariable instanceof MetricVariable) 
				|| toType.equals(AnyMetricVariable.class)
				|| possibleMetricVariable.getClass().equals(toType))
			return possibleMetricVariable;
		
		MetricVariable metricVariable = (MetricVariable) possibleMetricVariable;
		
		return parseVariable(metricVariable.getName(), toType);
	}

	private ValueComputable parseVariable(String variableName, Class<? extends MetricVariable> type) throws ConfigurationException, ParseException {
		if(variableNames.contains(variableName)) {
			Optional<Class<? extends MetricVariable>> typeFromAggregation = MetricVariable.returnTypeFromAggregation(variablesProperties.getSubset(variableName));
			if(typeFromAggregation.isPresent()) {
				if(!type.equals(AnyMetricVariable.class) && !typeFromAggregation.get().equals(type))
					throw new ParseException("Variable "+variableName+" has type "+typeFromAggregation.get().getSimpleName()+" because of its aggregation operation, "
							+ "but in the equation there is a function that uses it as type " + type.getSimpleName() , pos);
				
				type = typeFromAggregation.get();
			}
			
			if(!variables.containsKey(variableName)) {				
				if(type == null || type.equals(AnyMetricVariable.class))
					variables.put(variableName, new AnyMetricVariable(variableName));
				else if(type.equals(FloatMetricVariable.class))
					variables.put(variableName, new FloatMetricVariable(variableName));
				else if(type.equals(BooleanMetricVariable.class))
					variables.put(variableName, new BooleanMetricVariable(variableName));
				else if(type.equals(StringMetricVariable.class))
					variables.put(variableName, new StringMetricVariable(variableName));
				
				variables.get(variableName).config(variablesProperties.getSubset(variableName));
			}else{
				MetricVariable previousVariable = variables.get(variableName);
				
				if(previousVariable.getClass().equals(AnyMetricVariable.class) && !type.equals(AnyMetricVariable.class)) {
					if(type.equals(FloatMetricVariable.class))
						variables.put(variableName, new FloatMetricVariable(variableName));
					else if(type.equals(BooleanMetricVariable.class))
						variables.put(variableName, new BooleanMetricVariable(variableName));
					else if(type.equals(StringMetricVariable.class))
						variables.put(variableName, new StringMetricVariable(variableName));
					
					variables.get(variableName).config(variablesProperties.getSubset(variableName));
				}else if(!type.equals(AnyMetricVariable.class) && !previousVariable.getClass().equals(type))
					throw new ParseException("Variable "+variableName+" is used by functions that expect it as different types "
								+ "(" + variables.get(variableName).getClass().getSimpleName() + ", " + type.getSimpleName()+")" , pos);
			}
			
			return variables.get(variableName);
		}else
			throw new ParseException("Unknown variable: " + variableName, pos);
	}

	private ValueComputable parseFunction(String functionRepresentation) throws ParseException, ConfigurationException {
		Map<String, FunctionCaller> functions = new HashMap<>();
		new IfStringFunc.Caller().register(functions);
		new IfBoolFunc.Caller().register(functions);
		new IfFloatFunc.Caller().register(functions);
		
		new SqrtFunc.Caller().register(functions);
		new SinFunc.Caller().register(functions);
		new CosFunc.Caller().register(functions);
		new TanFunc.Caller().register(functions);
		new AbsFunc.Caller().register(functions);
		
		new ConcatFunc.Caller().register(functions);
		new TrimFunc.Caller().register(functions);
		
		if(functions.containsKey(functionRepresentation)) {
			FunctionCaller caller = functions.get(functionRepresentation);
			
			ValueComputable[] arguments = parseFunctionArguments(caller.getArgumentTypes());
			
			return caller.call(arguments);
		}else{
			throw new ParseException("Unknown function: " + functionRepresentation, pos);
		}
	}

	private ValueComputable[] parseFunctionArguments(Class<? extends Value>[] argumentTypes) throws ConfigurationException, ParseException {
		ValueComputable[] arguments = new ValueComputable[argumentTypes.length];
		
		if(!eat("("))
			throw new ParseException("Expected \"(\" after function name.", pos);
		
		int i = 0;
		for (Class<? extends Value> argumentType : argumentTypes) {
			arguments[i] = typeVariable(parseExpression(), MetricVariable.from(argumentType));
			
			eat(",");
			i++;
		}
		
		if(!eat(")"))
			throw new ParseException("Expected \")\" after function arguments.", pos);
		
		return arguments;
	}

}
