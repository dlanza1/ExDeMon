package ch.cern.spark.metrics;

import java.io.IOException;

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
import ch.cern.spark.metrics.notifications.NotificationStoresRDD;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;
import ch.cern.spark.metrics.source.MetricsSource;
import ch.cern.spark.metrics.store.MetricStoresRDD;

public final class Driver {
    
    public static String CHECKPOINT_DIR_PARAM = "checkpoint.dir";  
    public static String CHECKPOINT_DIR_DEFAULT = "/tmp/";  
    
    public static String BATCH_INTERVAL_PARAM = "spark.batch.time";  

	public Driver() {
	}

	public static void main(String[] args) throws Exception {

	    String propertyFilePath = args[0];
	    
        final Properties.Expirable props = new Properties.Expirable(propertyFilePath);

		final SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("MonitorMetricsStreamingJob");
		sparkConf.runLocallyIfMasterIsNotConfigured();
		sparkConf.addProperties(props.get(), "spark.");

		removeSparkCheckpointDir(props);
		
        JavaStreamingContext ssc = createNewStreamingContext(sparkConf, props);
		
		// Start the computation
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    private static void removeSparkCheckpointDir(Properties.Expirable props) throws IOException {
        Path path = new Path(getCheckpointDir(props) + "/checkpoint/");
        
        FileSystem fs = FileSystem.get(new Configuration());
        
        if(fs.exists(path))
            fs.delete(path, true);
        
    }

    protected static JavaStreamingContext createNewStreamingContext(SparkConf sparkConf, Properties.Expirable props) 
            throws Exception {
	    
        Long batchInterval = props.get().getLong(BATCH_INTERVAL_PARAM);
        if(batchInterval == null)
            batchInterval = 30l;
        
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
		ssc.checkpoint(getCheckpointDir(props) + "/checkpoint/");
		
		MetricsSource metricSource = (MetricsSource) ComponentManager.build(Type.SOURCE, props.get().getSubset("source"));
		MetricsS metrics = metricSource.createMetricsStream(ssc);
		
        MetricStoresRDD initialMetricStores = MetricStoresRDD.load(getCheckpointDir(props), ssc.sparkContext());
		AnalysisResultsS results = metrics.monitor(props, initialMetricStores);
		
		AnalysisResultsSink analysisResultsSink = 
		        (AnalysisResultsSink) ComponentManager.build(Type.ANALYSIS_RESULTS_SINK, props.get().getSubset("results.sink"));
		results.sink(analysisResultsSink);
		
		NotificationStoresRDD initialNotificationStores = NotificationStoresRDD.load(getCheckpointDir(props), ssc.sparkContext());
		NotificationsS notifications = results.notifications(props, initialNotificationStores);
		
		NotificationsSink notificationsSink = 
		        (NotificationsSink) ComponentManager.build(Type.NOTIFICATIONS_SINK, props.get().getSubset("notifications.sink"));
		notifications.sink(notificationsSink);
		
		return ssc;
	}
    
    public static String getCheckpointDir(Expirable propertiesExp) throws IOException {
        return propertiesExp.get().getProperty(Driver.CHECKPOINT_DIR_PARAM, Driver.CHECKPOINT_DIR_DEFAULT);
    }

}
