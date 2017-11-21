package ch.cern.spark.metrics.results.sink.types;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

@RegisterComponent("influx")
public class InfluxAnalysisResultsSink extends AnalysisResultsSink {

	private static final long serialVersionUID = -5981419650023205174L;


    @Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
    }
    
    @Override
    public void sink(Stream<AnalysisResult> outputStream) {
        JavaDStream<String> stream = outputStream.asJSON().asString().asJavaDStream();
        
        
    }

}
