package ch.cern.spark.metrics;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.spark.Component.Type;
import ch.cern.spark.ComponentManager;
import ch.cern.spark.Properties;
import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;
import ch.cern.spark.metrics.source.MetricsSource;

public final class Driver {
    
    public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";  
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";  
    
    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";
    
    private Properties.Expirable properties;
    
    private SparkConf sparkConf;

	public Driver(Properties.Expirable props) throws IOException {
	    this.properties = props;

        sparkConf = new SparkConf();
        sparkConf.setAppName("MetricsMonitorStreamingJob");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(this.properties.get(), "spark.");
	}

	public static void main(String[] args) throws Exception {

	    if(args.length != 1)
	        throw new RuntimeException("A single argument must be specified with the path to the configuration file.");
	    
	    String propertyFilePath = args[0];
	    Properties.Expirable props = new Properties.Expirable(propertyFilePath);
	    
	    Driver driver = new Driver(props);

		driver.removeSparkCheckpointDir();
		
        JavaStreamingContext ssc = driver.createNewStreamingContext();
		
		// Start the computation
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    private void removeSparkCheckpointDir() throws IOException {
        Path path = new Path(getCheckpointDir(properties) + "/checkpoint/");
        
        FileSystem fs = FileSystem.get(new Configuration());
        
        if(fs.exists(path))
            fs.delete(path, true);
        
    }

    protected JavaStreamingContext createNewStreamingContext() 
            throws Exception {
        
		JavaStreamingContext ssc = newStreamingContext();
		
		MetricsSource metricSource = getMetricSource();
		Optional<AnalysisResultsSink> analysisResultsSink = getAnalysisResultsSink();
		Optional<NotificationsSink> notificationsSink = getNotificationsSink();
	    
		MetricsS metrics = metricSource.createMetricsStream(ssc);
		
		AnalysisResultsS results = metrics.monitor(properties);

		analysisResultsSink.ifPresent(results::sink);
		
		NotificationsS notifications = results.notify(properties);
		
    		notificationsSink.ifPresent(notifications::sink);
        
        if(!analysisResultsSink.isPresent() && !notificationsSink.isPresent())
            throw new RuntimeException("At least one sink must be configured");
		
		return ssc;
	}
    
    private Optional<NotificationsSink> getNotificationsSink() throws Exception {
    		Properties notificationsSinkProperties = properties.get().getSubset("notifications.sink");

    		Optional<NotificationsSink> notificationsSink = ComponentManager.buildOptional(Type.NOTIFICATIONS_SINK, notificationsSinkProperties);
		
    		return notificationsSink;
	}

	private Optional<AnalysisResultsSink> getAnalysisResultsSink() throws Exception {
		Properties analysisResultsSinkProperties = properties.get().getSubset("results.sink");
		
		Optional<AnalysisResultsSink> analysisResultsSink = ComponentManager.buildOptional(Type.ANALYSIS_RESULTS_SINK, analysisResultsSinkProperties);
		
		return analysisResultsSink;
	}

	private MetricsSource getMetricSource() throws Exception {
    		Properties metricSourceProperties = properties.get().getSubset("source");
		
    		if(!metricSourceProperties.isTypeDefined())
		    throw new RuntimeException("A metric source must be configured");
		
		MetricsSource metricSource = ComponentManager.build(Type.SOURCE, metricSourceProperties);
		
		return metricSource;
	}

	private JavaStreamingContext newStreamingContext() throws IOException {
    		long batchInterval = properties.get().getLong(BATCH_INTERVAL_PARAM, 30);
		
    		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		
		ssc.checkpoint(getCheckpointDir(properties) + "/checkpoint/");
		
		return ssc;
	}

	public static String getCheckpointDir(Expirable propertiesExp) throws IOException {
        return propertiesExp.get().getProperty(Driver.CHECKPOINT_DIR_PARAM, Driver.CHECKPOINT_DIR_DEFAULT);
    }

}
