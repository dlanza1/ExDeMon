package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ClassNameAlias("defined-metric-status")
@ToString
@EqualsAndHashCode(callSuper=false)
public class VariableStatuses extends StatusValue {
	
	private static final long serialVersionUID = 3020679839103994736L;

	private Map<String, VariableStatus> statuses;
	
	private Instant processedBatchTime;
	
	public VariableStatuses() {
		statuses = new HashMap<>();
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

	public void put(String varName, VariableStatus status) {
		statuses.put(varName, status);
	}

	public boolean containsVariable(String StatusValue) {
		return statuses.containsKey(StatusValue);
	}

	public VariableStatus get(String varName) {
		return statuses.get(varName);
	}

}
