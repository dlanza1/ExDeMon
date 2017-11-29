package ch.cern.spark.metrics;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.spark.PairStream;
import ch.cern.spark.RDD;
import ch.cern.spark.SparkConf;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.defined.DefinedMetrics;
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
    
	private List<MetricsSource> metricSources;
	private Optional<AnalysisResultsSink> analysisResultsSink;
	private List<NotificationsSink> notificationsSinks;

	public Driver(Properties properties) throws Exception {
		removeSparkCheckpointDir(properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT));
		
        ssc = newStreamingContext(properties);

        metricSources = getMetricSources(properties);
		analysisResultsSink = getAnalysisResultsSink(properties);
		notificationsSinks = getNotificationsSinks(properties);
		
		if(!analysisResultsSink.isPresent() && notificationsSinks.size() == 0)
            throw new ConfigurationException("At least one sink must be configured");
	}

	public static void main(String[] args) throws Exception {

	    if(args.length != 1)
	        throw new ConfigurationException("A single argument must be specified with the path to the configuration file.");
	    
	    String propertyFilePath = args[0];
	    Properties properties = Properties.fromFile(propertyFilePath);
	    properties.setDefaultPropertiesSource(propertyFilePath);
	    
	    Driver driver = new Driver(properties);
		
        JavaStreamingContext ssc = driver.createNewStreamingContext(properties.getSubset(PropertiesSource.CONFIGURATION_PREFIX));
		
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

    protected JavaStreamingContext createNewStreamingContext(Properties propertiesSourceProps) throws Exception {
	    
    		Stream<Metric> metrics = getMetricsStream();
    		
		metrics = metrics.union(DefinedMetrics.generate(metrics, propertiesSourceProps));
		metrics.asJavaDStream().cache();
		
		Stream<AnalysisResult> results = Monitors.analyze(metrics, propertiesSourceProps);
		results.asJavaDStream().cache();

		analysisResultsSink.ifPresent(results::sink);
		
		Stream<Notification> notifications = Monitors.notify(results, propertiesSourceProps);
		notifications.asJavaDStream().cache();
		
    		notificationsSinks.stream().forEach(notifications::sink);
		
		return ssc;
	}

	public Stream<Metric> getMetricsStream() {
		return metricSources.stream()
				.map(source -> source.createStream(ssc))
				.reduce((str, stro) -> str.union(stro)).get();
	}

	private Optional<AnalysisResultsSink> getAnalysisResultsSink(Properties properties) throws Exception {
		Properties analysisResultsSinkProperties = properties.getSubset("results.sink");
		
		return ComponentManager.buildOptional(Type.ANALYSIS_RESULTS_SINK, analysisResultsSinkProperties);
	}

	private List<MetricsSource> getMetricSources(Properties properties) throws Exception {
		List<MetricsSource> metricSources = new LinkedList<>();
		
    		Properties metricSourcesProperties = properties.getSubset("metrics.source");
		
    		Set<String> ids = metricSourcesProperties.getUniqueKeyFields();
    		
    		for (String id : ids) {
    			Properties props = metricSourcesProperties.getSubset(id);
			
    			MetricsSource source = ComponentManager.build(Type.METRIC_SOURCE, props);
    			source.setId(id);
    			
    			metricSources.add(source);
		}
    		
    		if(metricSources.size() < 1)
		    throw new ConfigurationException("At least one metric source must be configured");
		
		return metricSources;
	}
	
	private List<NotificationsSink> getNotificationsSinks(Properties properties) throws Exception {
		List<NotificationsSink> notificationsSinks = new LinkedList<>();
		
    		Properties sinkProperties = properties.getSubset("notifications.sink");
		
    		Set<String> ids = sinkProperties.getUniqueKeyFields();
    		
    		for (String id : ids) {
    			Properties props = sinkProperties.getSubset(id);
			
    			NotificationsSink sink = ComponentManager.build(Type.NOTIFICATIONS_SINK, props);
    			sink.setId(id);
    			
    			notificationsSinks.add(sink);
		}
		
		return notificationsSinks;
	}
	
	public JavaStreamingContext newStreamingContext(Properties properties) throws IOException, ConfigurationException {
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("MetricsMonitorStreamingJob");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(properties, "spark.");
        
        String checkpointDir = properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT);
        sparkConf.set(RDD.CHECKPPOINT_DIR_PARAM, checkpointDir);
        
        Duration dataExpirationPeriod = properties.getPeriod(DATA_EXPIRATION_PARAM, DATA_EXPIRATION_DEFAULT);
        sparkConf.set(PairStream.CHECKPPOINT_DURATION_PARAM, dataExpirationPeriod.toString());
        
		sparkConf.addProperties(properties, PropertiesSource.CONFIGURATION_PREFIX);
        
    		long batchInterval = properties.getPeriod(BATCH_INTERVAL_PARAM, Duration.ofMinutes(1)).getSeconds();
		
    		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		
		ssc.checkpoint(checkpointDir + "/checkpoint/");
		
		return ssc;
	}
	
	public JavaStreamingContext getJavaStreamingContext() {
		return ssc;
	}

}
