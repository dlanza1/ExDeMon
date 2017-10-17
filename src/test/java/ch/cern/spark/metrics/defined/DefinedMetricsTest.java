package ch.cern.spark.metrics.defined;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;

public class DefinedMetricsTest {

    public static DefinedMetrics mockedExpirable() {
    		DefinedMetrics propExp = mock(DefinedMetrics.class, withSettings().serializable());
        
        try {
            when(propExp.get()).thenReturn(new HashMap<>());
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return propExp;
    }
    
}
