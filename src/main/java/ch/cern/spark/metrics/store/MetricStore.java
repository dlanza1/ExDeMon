package ch.cern.spark.metrics.store;

import java.io.Serializable;
import java.util.Date;

import org.apache.spark.streaming.Time;

public class MetricStore implements Serializable{

    private static final long serialVersionUID = 7584623306853907073L;
    
    private Date lastestTimestamp;
    
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

    public void updateLastestTimestamp(Date newTimestamp) {
        if(lastestTimestamp == null || lastestTimestamp.getTime() < newTimestamp.getTime())
            lastestTimestamp = newTimestamp;
    }
    
    public Date getLastestTimestamp() {
        return lastestTimestamp;
    }

    public long elapsedTimeFromLastMetric(Time time) {
        if(lastestTimestamp == null)
            return 0;
        
        long elapsed = time.milliseconds() - lastestTimestamp.getTime();
        
        if(elapsed > 0)
            return elapsed / 1000;
        else
            return 0;
    }

}
