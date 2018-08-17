package ch.cern.exdemon.metrics.defined.equation.var;

import java.util.Optional;

import ch.cern.exdemon.components.ConfigurationResult;
import lombok.Getter;

public class VariableCreationResult {
    
    private Variable variable = null;
    
    @Getter
    private ConfigurationResult configResult;

    public VariableCreationResult(Variable variable, ConfigurationResult configResult) {
        super();
        
        this.configResult = configResult;
        
        if(configResult.getErrors().isEmpty())
            this.variable = variable;
    }
    
    public Optional<Variable> getVariable(){
        return Optional.ofNullable(variable);
    }
    
}
