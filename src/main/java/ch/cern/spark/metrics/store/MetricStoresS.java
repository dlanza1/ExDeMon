package ch.cern.spark.metrics.store;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
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
        stream().foreachRDD(new VoidFunction2<JavaPairRDD<MonitorIDMetricIDs,MetricStore>, Time>() {
            private static final long serialVersionUID = -4071427105537124717L;

            @Override
            public void call(JavaPairRDD<MonitorIDMetricIDs, MetricStore> rdd, Time time) throws Exception {
                MetricStoresRDD stores = new MetricStoresRDD(rdd);
                
                stores.save(storing_path);
            }
        });
    }

    public AnalysisResultsS missingMetricResults(final Expirable propertiesExp) {
        return new AnalysisResultsS(stream().transform(new Function2<JavaPairRDD<MonitorIDMetricIDs,MetricStore>, Time, JavaRDD<AnalysisResult>>(){
            private static final long serialVersionUID = 3859133470714157466L;

            @Override
            public JavaRDD<AnalysisResult> call(JavaPairRDD<MonitorIDMetricIDs, MetricStore> rdd, Time time) throws Exception {
                return rdd.flatMap(new ComputeMissingMetricResultsF(propertiesExp, time));
            }
            
        }));
    }

}
