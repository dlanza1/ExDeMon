package ch.cern.spark.metrics.store;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class MetricStore implements Serializable{

    private static final long serialVersionUID = 7584623306853907073L;
    
    private Instant lastestTimestamp;
    
    private Store preAnalysisStore;
    
    private Store analysisStore;
    
    public MetricStore(){
    }

    public Store getPreAnalysisStore() {
        return preAnalysisStore;
    }

    public Store getAnalysisStore() {
        return analysisStore;
    }

    public void setPreAnalysisStore(Store store) {
        this.preAnalysisStore = store;
    }
    
    public void setAnalysisStore(Store store) {
        this.analysisStore = store;
    }

    public void updateLastestTimestamp(Instant time) {
        if(lastestTimestamp == null || lastestTimestamp.isBefore(time))
            lastestTimestamp = time;
    }
    
    public Instant getLastestTimestamp() {
        return lastestTimestamp;
    }

    public Duration elapsedTimeFromLastMetric(Instant time) {
        if(lastestTimestamp == null)
            return Duration.ZERO;
        
        return Duration.between(lastestTimestamp, time).abs();
    }

}
