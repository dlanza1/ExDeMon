package ch.cern.spark.flume;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

public class FilterFlumeEventsByHeader implements Function<FlumeEvent, Boolean> {

	private static final long serialVersionUID = 277844960326746293L;
	
	private String header;
	private String value;

	public FilterFlumeEventsByHeader(String header, String value) {
		this.header = header;
		this.value = value;
	}

	public Boolean call(FlumeEvent event) throws Exception {
		if(value == null)
			return !event.getHeaders().containsKey(header);
		
		return event.getHeaders().containsKey(header) && event.getHeaders().get(header).equals(value);
	}
	
	public static JavaDStream<FlumeEvent> apply(JavaDStream<FlumeEvent> events, String header, String value) {
		return events.filter(new FilterFlumeEventsByHeader(header, value));
	}

}
