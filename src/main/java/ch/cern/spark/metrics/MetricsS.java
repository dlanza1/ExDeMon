package ch.cern.spark.metrics;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.metrics.monitors.Monitors;

public class MetricsS extends JavaDStream<Metric> {

    private static final long serialVersionUID = -3384733211318788314L;

    public MetricsS(JavaDStream<Metric> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public JavaPairDStream<MonitorIDMetricIDs, Metric> withID(Monitors monitorsCache) {
        return flatMapToPair(new ComputeIDsForMetricsF(monitorsCache));
    }
    
    public <R> R mapS(Function<MetricsS, R> f) throws Exception {
    		return f.call(this);
    }
    
}
