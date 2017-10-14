package ch.cern.spark.metrics.store;

import java.io.IOException;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.metrics.ComputeMissingMetricResultsF;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import scala.Tuple2;

public class MetricStoresS extends JavaDStream<Tuple2<MonitorIDMetricIDs, MetricStore>> {

    private static final long serialVersionUID = -6600062851176859489L;
    
    public MetricStoresS(JavaDStream<Tuple2<MonitorIDMetricIDs, MetricStore>> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public void save(final String storing_path) throws IOException {
    		foreachRDD(rdd -> new MetricStoresRDD(rdd).save(storing_path));
    }

    public AnalysisResultsS missingMetricResults(final PropertiesCache propertiesExp) {
    		JavaDStream<AnalysisResult> results = transform((rdd, time) -> rdd.flatMap(
    					new ComputeMissingMetricResultsF(propertiesExp, time))
    				);
    		
		return new AnalysisResultsS(results);
    }

}
