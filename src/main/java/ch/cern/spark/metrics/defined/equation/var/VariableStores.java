package ch.cern.spark.metrics.defined.equation.var;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;

import ch.cern.spark.metrics.store.Store;

public class VariableStores extends HashMap<String, Store> implements Serializable{
	
	private static final long serialVersionUID = 3020679839103994736L;

	private Instant processedBatchTime;
	
	public VariableStores() {
		super();
	}

	public boolean newProcessedBatchTime(Instant time) {
		if(this.processedBatchTime == null) {
			this.processedBatchTime = time;
			
			return true;
		}else {
			if(this.processedBatchTime.compareTo(time) == 0) {
				return false;
			}else{
				this.processedBatchTime = time;
				
				return true;
			}
		}
	}

}
