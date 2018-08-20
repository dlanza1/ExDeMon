package ch.cern.exdemon.monitor.trigger.action.actuator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.ActionTest;
import ch.cern.exdemon.monitor.trigger.action.template.Template;
import ch.cern.properties.ConfigurationException;

public class ActuatorTest {

    @Test
    public void filter() throws ConfigurationException {
        TestActuator actuator = new TestActuator();

        Action action = ActionTest.DUMMY;
        
        action.setActuatorIDs(new HashSet<>(Arrays.asList("aa", "bb")));
        assertFalse(actuator.shouldBeProcess(action));
        
        action.setActuatorIDs(new HashSet<>(Arrays.asList("ALL")));
        assertTrue(actuator.shouldBeProcess(action));
        
        action.setActuatorIDs(new HashSet<>(Arrays.asList("test")));
        assertTrue(actuator.shouldBeProcess(action));
        
        action.setActuatorIDs(new HashSet<>(Arrays.asList("aa", "test")));
        assertTrue(actuator.shouldBeProcess(action));
        
        action.setActuatorIDs(new HashSet<>(Arrays.asList("aa", "ALL")));
        assertTrue(actuator.shouldBeProcess(action));
    }
    
    @Test
    public void template() {
        String template = "<monitor_id> <trigger_id> <attributes:.*> <attribute_value:a> <datetime> <reason> <tags> <tags:b>";

        Action action = ActionTest.DUMMY;
        action.setMonitor_id("M_ID");
        action.setTrigger_id("T_ID");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "a1");
        metric_attributes.put("b", "b2");
        action.setMetric_attributes(metric_attributes );
        action.setCreation_timestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
        action.setReason("reason_action");
        Map<String, String> tags = new HashMap<>();
        tags.put("a", "a_tag");
        tags.put("b", "b_tag");
        action.setTags(tags );
        
        String result = null;
        try {
            result = Template.apply(template, action);
        } catch (Exception e) {
            fail();
        }

        assertEquals("M_ID "
                + "T_ID " 
                + "\na = a1" 
                + "\nb = b2 "
                + "a1 "
                + "2007-12-03 11:15:30 "
                + "reason_action " 
                + "\na = a_tag" 
                + "\nb = b_tag "
                + "b_tag", result);
    }

    public static class TestActuator extends Actuator {

        private static final long serialVersionUID = 1281273704318553809L;

        public TestActuator() {
            setId("test");
        }

        @Override
        protected void run(Action action) {
            
        }

    }

}
