package ch.cern.spark.metrics;

import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import ch.cern.spark.Stream;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.store.MetricStore;
import ch.cern.spark.metrics.store.MetricStoresS;

public class MetricStatusesS extends Stream<JavaMapWithStateDStream<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult>>{

    private static final long serialVersionUID = -2190436746660329929L;

    public MetricStatusesS(JavaMapWithStateDStream<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult> stream) {
        super(stream);
    }

    public MetricStoresS getMetricStoresStream() {
        return new MetricStoresS(stream().stateSnapshots());
    }
    
    public AnalysisResultsS getAnalysisResultsStream(){
        return new AnalysisResultsS(stream());
    }

}
