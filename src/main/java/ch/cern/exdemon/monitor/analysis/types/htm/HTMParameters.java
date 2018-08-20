package ch.cern.exdemon.monitor.analysis.types.htm;

import static org.numenta.nupic.algorithms.Anomaly.KEY_ESTIMATION_SAMPLES;
import static org.numenta.nupic.algorithms.Anomaly.KEY_LEARNING_PERIOD;

import java.util.HashMap;
import java.util.Map;

import org.numenta.nupic.Parameters;
import org.numenta.nupic.Parameters.KEY;
import org.numenta.nupic.model.Persistable;
import org.numenta.nupic.util.Tuple;

public class HTMParameters implements Persistable{
	private static final long serialVersionUID = 4118240112485895557L;
	private Parameters p;
	
	public void setModelParameters(float minValue, float maxValue, boolean timeOfDay, 
			boolean dateOfWeek, boolean isWeekend, String timeformat) {
		Map<String, Map<String, Object>> fieldEncodings = setupEncoderMap(
                null,
                0, // n
                0, // w
                0, 0, 0, 0, null, null, null,
                "timestamp", "datetime", "DateEncoder");
        fieldEncodings = setupEncoderMap(
                fieldEncodings, 
                50, 
                21, 
                minValue, maxValue, 0, 0.1, null, Boolean.TRUE, null, 
                "value", "float", "ScalarEncoder");
        
        if(timeOfDay)
        	fieldEncodings.get("timestamp").put(KEY.DATEFIELD_TOFD.getFieldName(), new Tuple(21,9.5));
        if(dateOfWeek)
        	fieldEncodings.get("timestamp").put(KEY.DATEFIELD_DOFW.getFieldName(), new Tuple(21,9.5));
        if(isWeekend)
        	fieldEncodings.get("timestamp").put(KEY.DATEFIELD_WKEND.getFieldName(), new Tuple(21,9.5));
        
        fieldEncodings.get("timestamp").put(KEY.DATEFIELD_PATTERN.getFieldName(), timeformat);

        p = Parameters.getEncoderDefaultParameters();
        p.set(KEY.GLOBAL_INHIBITION, true);
        p.set(KEY.COLUMN_DIMENSIONS, new int[] { 2048 });
        p.set(KEY.INPUT_DIMENSIONS, new int[] { 2048 });
        p.set(KEY.CELLS_PER_COLUMN, 32);
        p.set(KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, 40.0);
        p.set(KEY.POTENTIAL_PCT, 0.85);
        p.set(KEY.SYN_PERM_CONNECTED,0.2);
        p.set(KEY.SYN_PERM_ACTIVE_INC, 0.003);
        p.set(KEY.SYN_PERM_INACTIVE_DEC, 0.0005);
        p.set(KEY.MAX_BOOST, 1.0);
        
        p.set(KEY.MAX_NEW_SYNAPSE_COUNT, 20);
        p.set(KEY.INITIAL_PERMANENCE, 0.21);
        p.set(KEY.PERMANENCE_INCREMENT, 0.04);
        p.set(KEY.PERMANENCE_DECREMENT, 0.008);
        p.set(KEY.MIN_THRESHOLD, 13);
        p.set(KEY.ACTIVATION_THRESHOLD, 20);
        
        p.set(KEY.CLIP_INPUT, true);
        p.set(KEY.FIELD_ENCODING_MAP, fieldEncodings);
	}
	
	public Parameters getParameters() {
		return p;
	}

    public static Map<String, Map<String, Object>> setupEncoderMap(
            Map<String, Map<String, Object>> map,
            int n, int w, double min, double max, double radius, double resolution, Boolean periodic,
            Boolean clip, Boolean forced, String fieldName, String fieldType, String encoderType) {

        if(map == null) {
            map = new HashMap<String, Map<String, Object>>();
        }
        Map<String, Object> inner = null;
        if((inner = map.get(fieldName)) == null) {
            map.put(fieldName, inner = new HashMap<String, Object>());
        }

        inner.put("n", n);
        inner.put("w", w);
        inner.put("minVal", min);
        inner.put("maxVal", max);
        inner.put("radius", radius);
        inner.put("resolution", resolution);

        if(periodic != null) inner.put("periodic", periodic);
        if(clip != null) inner.put("clipInput", clip);
        if(forced != null) inner.put("forced", forced);
        if(fieldName != null) inner.put("fieldName", fieldName);
        if(fieldType != null) inner.put("fieldType", fieldType);
        if(encoderType != null) inner.put("encoderType", encoderType);

        return map;
    }
    
    public static Map<String, Object> getAnomalyLikelihoodParams(){
	    Map<String, Object> p = new HashMap<>();
	    p.put(KEY_LEARNING_PERIOD, 100);
	    p.put(KEY_ESTIMATION_SAMPLES, 100);
	    return p;
    }
}
