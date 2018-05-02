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
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.spark.SparkConf;
import ch.cern.spark.ZookeeperJobListener;
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
import ch.cern.spark.metrics.trigger.action.actuator.Actuators;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.StatusesKeySocketReceiver;
import ch.cern.spark.status.storage.StatusesStorage;
import ch.cern.spark.status.storage.manager.ZookeeperStatusesOperationsReceiver;

public final class Driver {

    private transient final static Logger LOG = Logger.getLogger(Driver.class.getName());

    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";

    public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";

    private JavaStreamingContext ssc;

    private List<MetricsSource> metricSources;
    private Optional<AnalysisResultsSink> analysisResultsSink;

    public static String STATUSES_REMOVAL_SOCKET_PARAM = "statuses.removal.socket";
    private String statuses_removal_socket_host;
    private Integer statuses_removal_socket_port;
    
    public static String STATUSES_OPERATIONS_RECEIVER_PARAM = "spark.statuses.operations.zookeeper";
    public Properties statusesOperationsReceiverProperties;

    public Driver(Properties properties) throws Exception {
        removeSparkCheckpointDir(properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT));

        String removalSocket = properties.getProperty(STATUSES_REMOVAL_SOCKET_PARAM);
        if (removalSocket != null) {
            String[] host_port = removalSocket.trim().split(":");

            statuses_removal_socket_host = host_port[0];
            statuses_removal_socket_port = Integer.parseInt(host_port[1]);
        }

        statusesOperationsReceiverProperties = properties.getSubset(STATUSES_OPERATIONS_RECEIVER_PARAM);
        
        ssc = newStreamingContext(properties);

        metricSources = getMetricSources(properties);
        analysisResultsSink = getAnalysisResultsSink(properties);
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1)
            throw new ConfigurationException(
                    "A single argument must be specified with the path to the configuration file.");

        String propertyFilePath = args[0];
        Properties properties = Properties.fromFile(propertyFilePath);
        properties.setDefaultPropertiesSource(propertyFilePath);

        Driver driver = new Driver(properties);

        JavaStreamingContext ssc = driver
                .createNewStreamingContext(properties.getSubset(PropertiesSource.CONFIGURATION_PREFIX));

        // Start the computation
        ssc.start();

        ssc.sparkContext().sc().applicationId();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void removeSparkCheckpointDir(String mainDir) throws IOException {
        Path path = new Path(mainDir + "/checkpoint/");

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(path))
            fs.delete(path, true);
    }

    protected JavaStreamingContext createNewStreamingContext(Properties propertiesSourceProps) throws Exception {

        Optional<JavaDStream<StatusOperation<StatusKey, StatusValue>>> statusesOperationsOpt = getStatusesOperarions();
        Optional<JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>>> metricsStatusesOperationsOpt = Optional.empty();
        Optional<JavaDStream<StatusOperation<MonitorStatusKey, Metric>>> analysisStatusesOperationsOpt = Optional.empty();
        Optional<JavaDStream<StatusOperation<TriggerStatusKey, AnalysisResult>>> monitorsStatusesOperationsOpt = Optional.empty();
        if(statusesOperationsOpt.isPresent()) {
            metricsStatusesOperationsOpt = Optional.of(statusesOperationsOpt.get().filter(op -> op.getKey() == null || op.getKey() instanceof DefinedMetricStatuskey)
            																		.map(op -> new StatusOperation<>(op.getId(), (DefinedMetricStatuskey) op.getKey(), op.getOp())));
            analysisStatusesOperationsOpt = Optional.of(statusesOperationsOpt.get().filter(op -> op.getKey() == null || op.getKey() instanceof MonitorStatusKey)
            																		.map(op -> new StatusOperation<>(op.getId(), (MonitorStatusKey) op.getKey(), op.getOp())));
            monitorsStatusesOperationsOpt = Optional.of(statusesOperationsOpt.get().filter(op -> op.getKey() == null || op.getKey() instanceof TriggerStatusKey)
            																		.map(op -> new StatusOperation<>(op.getId(), (TriggerStatusKey) op.getKey(), op.getOp())));
        }
        
        JavaDStream<Metric> metrics = getMetricStream(propertiesSourceProps);
        
        metrics = metrics.union(DefinedMetrics.generate(metrics, propertiesSourceProps, metricsStatusesOperationsOpt));

        JavaDStream<AnalysisResult> results = Monitors.analyze(metrics, propertiesSourceProps, analysisStatusesOperationsOpt);

        analysisResultsSink.ifPresent(sink -> sink.sink(results));

        JavaDStream<Action> actions = Monitors.applyTriggers(results, propertiesSourceProps, monitorsStatusesOperationsOpt);

        Actuators.run(actions, propertiesSourceProps);

        return ssc;
    }

    private Optional<JavaDStream<StatusOperation<StatusKey, StatusValue>>> getStatusesOperarions() {
        if (statuses_removal_socket_host == null || statuses_removal_socket_port == null)
            return Optional.empty();
        
        JavaDStream<StatusOperation<StatusKey, StatusValue>> operations = ssc.receiverStream(new ZookeeperStatusesOperationsReceiver(statusesOperationsReceiverProperties));
        
        JavaReceiverInputDStream<StatusKey> keysToRemove = ssc.receiverStream(new StatusesKeySocketReceiver(statuses_removal_socket_host, statuses_removal_socket_port));
        operations = operations.union(keysToRemove.map(key -> new StatusOperation<>("old_remove", key, Op.REMOVE)));

        return Optional.of(operations);
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

        if (metricSources.size() < 1)
            throw new ConfigurationException("At least one metric source must be configured");

        return metricSources;
    }

    public JavaStreamingContext newStreamingContext(Properties properties) throws IOException, ConfigurationException {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("ExDeMon");
        sparkConf.runLocallyIfMasterIsNotConfigured();
        sparkConf.addProperties(properties, "spark.");

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.extraListeners", ZookeeperJobListener.class.getName());

        sparkConf.registerKryoClasses(Arrays.asList(
                                DefinedMetricStatuskey.class, 
                                MonitorStatusKey.class, 
                                TriggerStatusKey.class,
                                TriggerStatus.class, 
                                ValueHistory.class, 
                                VariableStatuses.class, 
                                AggregationValues.class)
                                .toArray(new Class[0]));

        String checkpointDir = properties.getProperty(CHECKPOINT_DIR_PARAM, CHECKPOINT_DIR_DEFAULT);

        if (!sparkConf.contains(StatusesStorage.STATUS_STORAGE_PARAM + ".type")) {
            sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "single-file");
            sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".path", checkpointDir + "/statuses");
        }

        long batchInterval = properties.getPeriod(BATCH_INTERVAL_PARAM, Duration.ofMinutes(1)).getSeconds();

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));

        ssc.checkpoint(checkpointDir + "/checkpoint/");

        try {
            ZookeeperJobListener streamingListener = new ZookeeperJobListener(
                    properties.getSubset("spark.streaming.listener"));

            ssc.sparkContext().sc().addSparkListener(streamingListener);
            ssc.addStreamingListener(streamingListener);
        } catch (Exception e) {
            LOG.error("Zookeeper job listener could not be attached: " + e.getMessage(), e);
        }

        return ssc;
    }

    public JavaStreamingContext getJavaStreamingContext() {
        return ssc;
    }

}
