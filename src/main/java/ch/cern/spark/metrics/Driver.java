package ch.cern.spark.metrics;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.Component.Type;
import ch.cern.ComponentManager;
import ch.cern.Properties;
import ch.cern.Properties.PropertiesCache;
import ch.cern.spark.PairStream;
import ch.cern.spark.RDD;
import ch.cern.spark.SparkConf;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;
import ch.cern.spark.metrics.source.MetricsSource;

public final class Driver {
    
    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";
    
	public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";  
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";  
    
    public static String DATA_EXPIRATION_PARAM = "data.expiration";
    public static Duration DATA_EXPIRATION_DEFAULT = Duration.ofHours(3);
    
    private JavaStreamingContext ssc;
    
	private MetricsSource metricSource;
	private Monitors monitors;
	private Optional<AnalysisResultsSink> analysisResultsSink;
	private Optional<NotificationsSink> notificationsSink;

	public Driver(PropertiesCache properties) throws Exception {
        ssc = newStreamingContext(properties.get());
        
        metricSource = getMetricSource(properties.get());
        monitors = getMonitors(properties, ssc);
		analysisResultsSink = getAnalysisResultsSink(properties.get());
		notificationsSink = getNotificationsSink(properties.get());
		
		if(!analysisResultsSink.isPresent() && !notificationsSink.isPresent())
            throw new RuntimeException("At least one sink must be configured");
	}

	private Monitors getMonitors(PropertiesCache properties, JavaStreamingContext ssc2) throws IOException {
		return new Monitors(properties);
	}

	public static void main(String[] args) throws Exception {

	    if(args.length != 1)
	        throw new RuntimeException("A single argument must be specified with the path to the configuration file.");
	    
	    String propertyFilePath = args[0];
	    PropertiesCache props = new PropertiesCache(propertyFilePath);
	    
	    Driver driver = new Driver(props);

		removeSparkCheckpointDir(props.get().getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT));
		
        JavaStreamingContext ssc = driver.createNewStreamingContext();
		
		// Start the computation
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    private static void removeSparkCheckpointDir(String mainDir) throws IOException {
        Path path = new Path(mainDir + "/checkpoint/");
        
        FileSystem fs = FileSystem.get(new Configuration());
        
        if(fs.exists(path))
            fs.delete(path, true);
        
    }

    protected JavaStreamingContext createNewStreamingContext() throws Exception {
	    
    		Stream<Metric> metrics = metricSource.createStream(ssc);
		
		Stream<AnalysisResult> results = metrics.mapS(monitors::analyze);
		
		analysisResultsSink.ifPresent(results::sink);
		
		Stream<Notification> notifications = results.mapS(monitors::notify);
		
    		notificationsSink.ifPresent(notifications::sink);
		
		return ssc;
	}
    
    private Optional<NotificationsSink> getNotificationsSink(Properties properties) throws Exception {
    		Properties notificationsSinkProperties = properties.getSubset("notifications.sink");

    		return ComponentManager.buildOptional(Type.NOTIFICATIONS_SINK, notificationsSinkProperties);
	}

	private Optional<AnalysisResultsSink> getAnalysisResultsSink(Properties properties) throws Exception {
		Properties analysisResultsSinkProperties = properties.getSubset("results.sink");
		
		return ComponentManager.buildOptional(Type.ANALYSIS_RESULTS_SINK, analysisResultsSinkProperties);
	}

	private MetricsSource getMetricSource(Properties properties) throws Exception {
    		Properties metricSourceProperties = properties.getSubset("source");
		
    		if(!metricSourceProperties.isTypeDefined())
		    throw new RuntimeException("A metric source must be configured");
		
		return ComponentManager.build(Type.SOURCE, metricSourceProperties);
	}

	private JavaStreamingContext newStreamingContext(Properties properties) throws IOException {
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("MetricsMonitorStreamingJob");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(properties, "spark.");
        
        String checkpointDir = properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT);
        sparkConf.set(RDD.CHECKPPOINT_DIR_PARAM, checkpointDir);
        
        Duration dataExpirationPeriod = properties.getPeriod(DATA_EXPIRATION_PARAM, DATA_EXPIRATION_DEFAULT);
        sparkConf.set(PairStream.CHECKPPOINT_DURATION_PARAM, dataExpirationPeriod.toString());
        
    		long batchInterval = properties.getLong(BATCH_INTERVAL_PARAM, 30);
		
    		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		
		ssc.checkpoint(checkpointDir + "/checkpoint/");
		
		return ssc;
	}

}
