package ch.cern.spark.metrics;

import java.io.IOException;

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

    public AnalysisResultsS monitor(final Expirable propertiesExp, MetricStoresRDD initialMetricStores) throws ClassNotFoundException, IOException {
    	
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithID = getMetricsWithIDStream(propertiesExp);
        
        MetricStatusesS statuses = UpdateMetricStatusesF.apply(metricsWithID, propertiesExp, initialMetricStores);
        AnalysisResultsS resultsFromAnalysis = statuses.getAnalysisResultsStream();
        
        MetricStoresS metricStores = statuses.getMetricStoresStatuses();
        
        AnalysisResultsS missingMetricsResults = metricStores.missingMetricResults(propertiesExp);
        
        metricStores.save(Driver.getCheckpointDir(propertiesExp));
        
        return new AnalysisResultsS(resultsFromAnalysis.union(missingMetricsResults));
    }

    public JavaPairDStream<MonitorIDMetricIDs, Metric> getMetricsWithIDStream(Expirable propertiesExp) {
        return flatMapToPair(new ComputeIDsForMetricsF(propertiesExp));
    }
    
}
