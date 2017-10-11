package ch.cern.spark.metrics.store;

import java.io.IOException;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.ComputeMissingMetricResultsF;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResultsS;

public class MetricStoresS extends Stream<JavaPairDStream<MonitorIDMetricIDs, MetricStore>> {

    private static final long serialVersionUID = -6600062851176859489L;
    
    public MetricStoresS(JavaPairDStream<MonitorIDMetricIDs, MetricStore> stream) {
        super(stream);
    }

    public void save(final String storing_path) throws IOException {
    		stream().foreachRDD(rdd -> new MetricStoresRDD(rdd).save(storing_path));
    }

    public AnalysisResultsS missingMetricResults(final Expirable propertiesExp) {
    		JavaDStream<AnalysisResult> results = stream().transform((rdd, time) -> rdd.flatMap(
    					new ComputeMissingMetricResultsF(propertiesExp, time))
    				);
    		
		return new AnalysisResultsS(results);
    }

}
