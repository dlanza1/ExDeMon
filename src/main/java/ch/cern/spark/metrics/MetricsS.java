package ch.cern.spark.metrics;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.store.MetricStoresRDD;
import ch.cern.spark.metrics.store.MetricStoresS;
import ch.cern.spark.metrics.store.UpdateMetricStatusesF;

public class MetricsS extends JavaDStream<Metric> {

    private static final long serialVersionUID = -3384733211318788314L;

    public MetricsS(JavaDStream<Metric> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public AnalysisResultsS monitor(Expirable propertiesExp) throws ClassNotFoundException, IOException {
    	
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithID = getMetricsWithIDStream(propertiesExp);
        
        MetricStoresRDD initialMetricStores = MetricStoresRDD.load(
										        		Driver.getCheckpointDir(propertiesExp), 
										        		JavaSparkContext.fromSparkContext(context().sparkContext()));
        
        MetricStatusesS statuses = UpdateMetricStatusesF.apply(metricsWithID, propertiesExp, initialMetricStores);
        
        MetricStoresS metricStores = statuses.getMetricStoresStatuses();
        metricStores.save(Driver.getCheckpointDir(propertiesExp));
        
        AnalysisResultsS missingMetricsResults = metricStores.missingMetricResults(propertiesExp);
        
        return statuses.getAnalysisResultsStream().union(missingMetricsResults);
    }

    public JavaPairDStream<MonitorIDMetricIDs, Metric> getMetricsWithIDStream(Expirable propertiesExp) {
        return flatMapToPair(new ComputeIDsForMetricsF(propertiesExp));
    }
    
}
