package ch.cern.exdemon.metrics.defined;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.Variable;
import ch.cern.properties.ConfigurationException;
import ch.cern.utils.TimeUtils;
import lombok.Getter;

public class When {

    private Duration period;

    @Getter
    private Collection<Variable> variables;

    private When() {
        this.period = null;
        this.variables = null;
    }
    
    public static When from(String config) throws ConfigurationException {
        return from(null, config);
    }
    
    public static When from(Map<String, Variable> variables, String config) throws ConfigurationException {
        When when = new When();
        
        //TODO DEPRECATED
        if(config.toUpperCase().equals("BATCH"))
            return from("1m");
        //TODO DEPRECATED
            
        if(config.toUpperCase().equals("ANY")) {
            when.variables = variables.values();
            
            return when;
        }
        
        try {
            when.period = TimeUtils.parsePeriod(config);
            
            return when;
        }catch(Exception e) {}
        
        Set<String> variableIDs = Arrays.stream(config.split(" ")).map(String::trim).collect(Collectors.toSet());
        when.variables = variables.values().stream()
                                            .filter(var -> variableIDs.contains(var.getName()))
                                            .collect(Collectors.toList());
        
        if(variableIDs.stream().filter(id -> !variables.keySet().contains(id)).count() > 0)
            throw new ConfigurationException("Variables listed in when parameter must be declared.");
        
        return when;
    }

    public boolean isTriggerAt(Instant batchTime) {
        return period != null;
    }

    public boolean isTriggerBy(Metric metric) {
        if(variables == null)
            return false;
        
        for (Variable variable : variables)
            if(variable.test(metric))
                return true;
        
        return false;
    }
    
}

