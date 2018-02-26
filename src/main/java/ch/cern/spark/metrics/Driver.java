package ch.cern.spark.metrics;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;
import ch.cern.spark.metrics.schema.MetricSchemas;
import ch.cern.spark.metrics.source.MetricsSource;
import ch.cern.spark.metrics.trigger.TriggerStatus;
import ch.cern.spark.metrics.trigger.TriggerStatusKey;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.metrics.trigger.action.actuator.Actuator;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusesKeyReceiver;
import ch.cern.spark.status.storage.StatusesStorage;

public final class Driver {
    
    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";
    
	public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";  
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";  
    
    private JavaStreamingContext ssc;
    
	private List<MetricsSource> metricSources;
	private Optional<AnalysisResultsSink> analysisResultsSink;
	private List<Actuator> actuators;

	public static String STATUSES_REMOVAL_SOCKET_PARAM = "statuses.removal.socket";
	private String statuses_removal_socket_host;
	private Integer statuses_removal_socket_port;

	public Driver(Properties properties) throws Exception {
		removeSparkCheckpointDir(properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT));
		
		String removalSocket = properties.getProperty(STATUSES_REMOVAL_SOCKET_PARAM);
		if(removalSocket != null) {
		    String[] host_port = removalSocket.trim().split(":");
		    
		    statuses_removal_socket_host = host_port[0];
		    statuses_removal_socket_port = Integer.parseInt(host_port[1]);
		}
		
        ssc = newStreamingContext(properties);

        metricSources = getMetricSources(properties);
		analysisResultsSink = getAnalysisResultsSink(properties);
		actuators = getActuators(properties);
		
		if(!analysisResultsSink.isPresent() && actuators.size() == 0)
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
	    
    		JavaDStream<Metric> metrics = getMetricStream(propertiesSourceProps);
    		
    		Optional<JavaDStream<StatusKey>> statusesToRemove = getStatusesToRemoveStream();
    		
		metrics = metrics.union(DefinedMetrics.generate(metrics, propertiesSourceProps, statusesToRemove));
		
		JavaDStream<AnalysisResult> results = Monitors.analyze(metrics, propertiesSourceProps, statusesToRemove);

		analysisResultsSink.ifPresent(sink -> sink.sink(results));
		
		JavaDStream<Action> actions = Monitors.applyTriggers(results, propertiesSourceProps, statusesToRemove);
		
    		actuators.stream().forEach(actuator -> actuator.sink(actions));
    		
    		//Make batch synchronous in case all output operations are async
    		actions.foreachRDD(rdd -> rdd.foreachPartition(it -> it.hasNext()));

		return ssc;
	}

	private Optional<JavaDStream<StatusKey>> getStatusesToRemoveStream() {
	    if(statuses_removal_socket_host == null || statuses_removal_socket_port == null)
	        return Optional.empty();

        return Optional.of(ssc.receiverStream(new StatusesKeyReceiver(statuses_removal_socket_host, statuses_removal_socket_port)));
    }

    public JavaDStream<Metric> getMetricStream(Properties propertiesSourceProps) {
		return metricSources.stream()
				.map(source -> MetricSchemas.generate(source.stream(ssc), propertiesSourceProps, source.getId()))
				.reduce((str, stro) -> str.union(stro)).get();
	}

	private Optional<AnalysisResultsSink> getAnalysisResultsSink(Properties properties) throws Exception {
		Properties analysisResultsSinkProperties = properties.getSubset("results.sink");
		
		return ComponentManager.buildOptional(Type.ANALYSIS_RESULTS_SINK, analysisResultsSinkProperties);
	}

	private List<MetricsSource> getMetricSources(Properties properties) throws Exception {
		List<MetricsSource> metricSources = new LinkedList<>();
		
    		Properties metricSourcesProperties = properties.getSubset("metrics.source");
		
    		Set<String> ids = metricSourcesProperties.getIDs();
    		
    		for (String id : ids) {
    			Properties props = metricSourcesProperties.getSubset(id);
			
    			MetricsSource source = ComponentManager.build(Type.METRIC_SOURCE, id, props);
    			source.setId(id);
    			
    			metricSources.add(source);
		}
    		
    		if(metricSources.size() < 1)
		    throw new ConfigurationException("At least one metric source must be configured");
		
		return metricSources;
	}
	
	private List<Actuator> getActuators(Properties properties) throws Exception {
		List<Actuator> actuators = new LinkedList<>();
		
		Properties actuatorsProperties = properties.getSubset("actuators");
		
		//TODO backward compatibility
    		Properties sinkPropertiesOld = properties.getSubset("notifications.sink");
    		actuatorsProperties.putAll(sinkPropertiesOld);
    		//TODO backward compatibility
		
    		Set<String> ids = actuatorsProperties.getIDs();
    		
    		for (String id : ids) {
    			Properties props = actuatorsProperties.getSubset(id);
			
    			Actuator sink = ComponentManager.build(Type.ACTUATOR, props);
    			sink.setId(id);
    			
    			actuators.add(sink);
		}
		
		return actuators;
	}
	
	public JavaStreamingContext newStreamingContext(Properties properties) throws IOException, ConfigurationException {
		
		SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("ExDeMon");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(properties, "spark.");
        
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        sparkConf.registerKryoClasses(Arrays.asList(
                                DefinedMetricStatuskey.class,
                                MonitorStatusKey.class,
                                TriggerStatusKey.class,
                                TriggerStatus.class,
                                ValueHistory.class,
                                VariableStatuses.class,
                                AggregationValues.class
                            ).toArray(new Class[0]));
        
        String checkpointDir = properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT);
        
        if(!sparkConf.contains(StatusesStorage.STATUS_STORAGE_PARAM + ".type")) {
        		sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "single-file");
        		sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".path", checkpointDir + "/statuses");
        }

    		long batchInterval = properties.getPeriod(BATCH_INTERVAL_PARAM, Duration.ofMinutes(1)).getSeconds();
		
    		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		
		ssc.checkpoint(checkpointDir + "/checkpoint/");
		
		return ssc;
	}
	
	public JavaStreamingContext getJavaStreamingContext() {
		return ssc;
	}

}
