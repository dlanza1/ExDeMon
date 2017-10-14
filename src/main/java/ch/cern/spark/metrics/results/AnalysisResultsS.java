package ch.cern.spark.metrics.results;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.json.JavaObjectToJSONObjectParser;
import ch.cern.spark.json.JsonS;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

public class AnalysisResultsS extends JavaDStream<AnalysisResult> {

    private static final long serialVersionUID = -7118785350719206804L;
    
    public AnalysisResultsS(JavaDStream<AnalysisResult> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public void sink(AnalysisResultsSink analysisResultsSink) {
        analysisResultsSink.sink(this);
    }
    
    public AnalysisResultsS union(AnalysisResultsS input) {
    		return new AnalysisResultsS(super.union(input));
    }

    public JavaPairDStream<NotificatorID, AnalysisResult> withNotificatorID(Monitors monitorsCache) {
        return flatMapToPair(new ComputeIDsForAnalysisF(monitorsCache));
    }

    public JsonS asJSON() {
        return new JsonS(JavaObjectToJSONObjectParser.apply(this));
    }
    
    public <R> R mapS(Function<AnalysisResultsS, R> f) throws Exception {
		return f.call(this);
    }

}
