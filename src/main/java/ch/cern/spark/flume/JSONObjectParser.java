package ch.cern.spark.flume;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.json.JSONObject;

public class JSONObjectParser implements Function<FlumeEvent, JSONObject> {
	
	private static final long serialVersionUID = -8345847182446763732L;

	private JSONObject.Parser parser = new JSONObject.Parser();
	
	public JSONObject call(FlumeEvent flumeEvent) throws Exception {
		return parser.parse(flumeEvent);
	}

	public static JavaDStream<JSONObject> apply(JavaDStream<FlumeEvent> flumeEvents) {
		return flumeEvents.map(new JSONObjectParser());
	}

}
