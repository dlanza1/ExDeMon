package ch.cern.exdemon.monitor.analysis.results.sink.types;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.http.HTTPSink;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.sink.AnalysisResultsSink;
import ch.cern.properties.Properties;

@RegisterComponentType("http")
public class HTTPAnalysisResultSink extends AnalysisResultsSink {

	private static final long serialVersionUID = 6368509840922047167L;

	private HTTPSink sink = new HTTPSink();

	@Override
	public ConfigurationResult config(Properties properties) {
		properties.setPropertyIfAbsent(HTTPSink.PARALLELIZATION_PARAM, "5");
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		
		return sink.config(properties);
	}
	
	@Override
	public void sink(JavaDStream<AnalysisResult> outputStream) {
		sink.sink(outputStream);
	}

}
