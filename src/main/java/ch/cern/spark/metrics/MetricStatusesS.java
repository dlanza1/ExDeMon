package ch.cern.spark.metrics;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import ch.cern.spark.MapWithStateStream;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.MetricStore;
import scala.Tuple2;

public class MetricStatusesS extends MapWithStateStream<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult>{

    private static final long serialVersionUID = -2190436746660329929L;

    public MetricStatusesS(JavaMapWithStateDStream<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult> stream) {
        super(stream);
    }

    public JavaDStream<Tuple2<MonitorIDMetricIDs, MetricStore>> getMetricStoresStatuses() {
        return stateSnapshots();
    }
    
    public JavaDStream<AnalysisResult> getAnalysisResultsStream(){
        return stream();
    }

}
