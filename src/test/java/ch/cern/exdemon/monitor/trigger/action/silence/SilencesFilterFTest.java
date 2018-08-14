package ch.cern.exdemon.monitor.trigger.action.silence;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.properties.Properties;

public class SilencesFilterFTest {

    private SilencesFilterF filterF;
    
    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("type", "test");
        ComponentsCatalog.init(properties);
        ComponentsCatalog.reset();
        
        filterF = new SilencesFilterF(null);
    }
    
    @Test
    public void silenceActionWithOneSilent() throws Exception {
        registerSilentWithFilterBy("val");
        
        assertFalse(filterF.call(createActionWithAttributeValue("val")));
    }
    
    @Test
    public void silenceActionWithSeveralSilents() throws Exception {
        registerSilentWithFilterBy("val_a");
        registerSilentWithFilterBy("val_b");
        
        assertTrue(filterF.call(createActionWithAttributeValue("val_1")));
        assertFalse(filterF.call(createActionWithAttributeValue("val_a")));
        assertFalse(filterF.call(createActionWithAttributeValue("val_b")));
    }
    
    @Test
    public void allowActionIfSilentIsNotInActivePeriod() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("from", "20230-12-10 12:34:12");
        properties.setProperty("filter.attribute.att", "val");
        ComponentsCatalog.register(Type.SILENCE, "", properties);
        
        assertTrue(filterF.call(createActionWithAttributeValue("val")));
    }
    
    @Test
    public void allowAction() throws Exception {
        registerSilentWithFilterBy("val");
        
        assertTrue(filterF.call(createActionWithAttributeValue("not_valid")));
    }
    
    @Test
    public void allowActionWithNoSilences() throws Exception {
        assertTrue(filterF.call(createActionWithAttributeValue("val1")));
    }

    private void registerSilentWithFilterBy(String attValue) {
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.att", attValue);
        
        ComponentsCatalog.register(Type.SILENCE, String.valueOf(Math.random()), properties);
    }
    
    private Action createActionWithAttributeValue(String attValue) {
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("att", attValue);
        
        Action action = new Action("", "", metric_attributes, "", new HashSet<>(), new HashMap<>(), new AnalysisResult());
        
        return action;
    }
    
}
