package ch.cern.spark.metrics.store;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

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

    public void updateLastestTimestamp(Instant time) {
        if(lastestTimestamp == null || lastestTimestamp.toInstant().isBefore(time))
            lastestTimestamp = Date.from(time);
    }
    
    public Instant getLastestTimestamp() {
        return lastestTimestamp != null ? lastestTimestamp.toInstant() : null;
    }

    public Duration elapsedTimeFromLastMetric(Instant time) {
        if(lastestTimestamp == null)
            return Duration.ZERO;
        
        return Duration.between(lastestTimestamp.toInstant(), time).abs();
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((analysisStore == null) ? 0 : analysisStore.hashCode());
		result = prime * result + ((lastestTimestamp == null) ? 0 : lastestTimestamp.hashCode());
		result = prime * result + ((preAnalysisStore == null) ? 0 : preAnalysisStore.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MetricStore other = (MetricStore) obj;
		if (analysisStore == null) {
			if (other.analysisStore != null)
				return false;
		} else if (!analysisStore.equals(other.analysisStore))
			return false;
		if (lastestTimestamp == null) {
			if (other.lastestTimestamp != null)
				return false;
		} else if (!lastestTimestamp.equals(other.lastestTimestamp))
			return false;
		if (preAnalysisStore == null) {
			if (other.preAnalysisStore != null)
				return false;
		} else if (!preAnalysisStore.equals(other.preAnalysisStore))
			return false;
		return true;
	}

}
