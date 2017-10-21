package ch.cern.spark.metrics.store;

import java.io.Serializable;
import java.util.Optional;

public class MetricStore implements Serializable{

    private static final long serialVersionUID = 7584623306853907073L;
    
    private Store preAnalysisStore;
    
    private Store analysisStore;
    
    public MetricStore(){
    }

    public Optional<Store> getPreAnalysisStore() {
        return Optional.ofNullable(preAnalysisStore);
    }

    public Optional<Store> getAnalysisStore() {
        return Optional.ofNullable(analysisStore);
    }

    public void setPreAnalysisStore(Store store) {
        this.preAnalysisStore = store;
    }
    
    public void setAnalysisStore(Store store) {
        this.analysisStore = store;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((analysisStore == null) ? 0 : analysisStore.hashCode());
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
		if (preAnalysisStore == null) {
			if (other.preAnalysisStore != null)
				return false;
		} else if (!preAnalysisStore.equals(other.preAnalysisStore))
			return false;
		return true;
	}

}
