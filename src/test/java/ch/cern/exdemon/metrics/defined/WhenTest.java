package ch.cern.exdemon.metrics.defined;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.Variable;
import ch.cern.properties.ConfigurationException;

public class WhenTest {
    
    @Test(expected=ConfigurationException.class)
    public void periodWithoutBatchDuration() throws ConfigurationException {
        When.from(null, "1m");
    }

    @Test
    public void everyBatch() throws ConfigurationException {
        When when = When.from(Duration.ofMinutes(1), "batch");

        Instant batchTime = Instant.parse("2017-12-03T10:15:00.00Z");
        
        assertTrue(when.isTriggerAt(batchTime));
        assertTrue(when.isTriggerAt(batchTime.plus(Duration.ofMinutes(1))));
        assertTrue(when.isTriggerAt(batchTime.plus(Duration.ofMinutes(2))));
    }

    @Test
    public void periodInMinutes() throws ConfigurationException {
        When when = When.from(Duration.ofMinutes(1), "10m");

        Instant time = Instant.parse("2007-12-03T10:00:00.00Z");
        
        assertTrue( when.isTriggerAt(time));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(1))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(2))));
        
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(9))));
        assertTrue( when.isTriggerAt(time.plus(Duration.ofMinutes(10))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(11))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(12))));
    }
    
    @Test
    public void periodInHours() throws ConfigurationException {
        When when = When.from(Duration.ofSeconds(10), "2h");

        Instant time = Instant.parse("2018-07-06T10:00:00.00Z");
        
        assertTrue( when.isTriggerAt(time));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofSeconds(10))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(1))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(2))));
        
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(60))));
        assertTrue( when.isTriggerAt(time.plus(Duration.ofMinutes(120))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(121))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(122))));
    }
    
    @Test
    public void periodInOddMinutes() throws ConfigurationException {
        When when = When.from(Duration.ofMinutes(1), "3m");

        Instant time = Instant.parse("2007-12-03T10:00:00.00Z");
        
        assertTrue (when.isTriggerAt(time.plus(Duration.ofMinutes(0))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(1))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(2))));
        assertTrue (when.isTriggerAt(time.plus(Duration.ofMinutes(3))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(4))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(5))));
        assertTrue (when.isTriggerAt(time.plus(Duration.ofMinutes(6))));
    }
    
    @Test
    public void periodAndPositiveDelay() throws ConfigurationException {
        When when = When.from(Duration.ofMinutes(1), "10m+3m");

        Instant time = Instant.parse("2007-12-03T10:00:00.00Z");
        
        assertFalse(when.isTriggerAt(time));
        
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(2))));
        assertTrue( when.isTriggerAt(time.plus(Duration.ofMinutes(3))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(4))));
        
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(12))));
        assertTrue( when.isTriggerAt(time.plus(Duration.ofMinutes(13))));
        assertFalse(when.isTriggerAt(time.plus(Duration.ofMinutes(14))));
    }
    
    @Test
    public void notTriggerByTimeIfVariable() throws ConfigurationException {
        Map<String, Variable> variables = new HashMap<>();
        variables.put("var", mock(Variable.class));
        When when = When.from(null, variables, "var");
        
        assertFalse(when.isTriggerAt(Instant.now()));
    }
    
    @Test(expected=ConfigurationException.class)
    public void periodNotMultiple() throws ConfigurationException {
        Duration batchDuration = Duration.ofMinutes(5);
        
        When.from(batchDuration, "3m");
    }
    
    @Test(expected=ConfigurationException.class)
    public void delayNotMultiple() throws ConfigurationException {
        Duration batchDuration = Duration.ofMinutes(5);
        
        When.from(batchDuration, "5m+7m");
    }
    
    @Test(expected=ConfigurationException.class)
    public void variableNotDeclared() throws ConfigurationException {
        Map<String, Variable> variables = new HashMap<>();
        
        When.from(null, variables, "var");
    }
    
    @Test
    public void triggerByAnyVariable() throws ConfigurationException {
        Map<String, Variable> variables = new HashMap<>();
        Variable var1 = mock(Variable.class);
        when(var1.getName()).thenReturn("var");
        when(var1.test(any())).thenReturn(true);
        variables.put("var", var1);
        When when = When.from(null, variables, "any");
        
        assertTrue(when.isTriggerBy(new Metric(Instant.now(), 1, new HashMap<>())));
    }
    
    @Test
    public void triggerByVariable() throws ConfigurationException {
        Map<String, Variable> variables = new HashMap<>();
        Variable var1 = mock(Variable.class);
        when(var1.getName()).thenReturn("var");
        when(var1.test(any())).thenReturn(true);
        variables.put("var", var1);
        When when = When.from(null, variables, "var");
        
        assertTrue(when.isTriggerBy(new Metric(Instant.now(), 1, new HashMap<>())));
    }
    
    @Test
    public void noTriggerByVariable() throws ConfigurationException {
        Map<String, Variable> variables = new HashMap<>();
        Variable var = mock(Variable.class);
        when(var.getName()).thenReturn("var");
        when(var.test(any())).thenReturn(false);
        variables.put("var", var);
        When when = When.from(null, variables, "var");
        
        assertFalse(when.isTriggerBy(new Metric(Instant.now(), 1, new HashMap<>())));
    }
    
}
