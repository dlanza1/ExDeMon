package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.util.Map;

public class DefinedMetricID implements Serializable{

	private static final long serialVersionUID = 2992810464329306975L;

	private String definedMetricName;
	
	private Map<String, String> groupByMetricIDs;

	public DefinedMetricID(String definedMetricName, Map<String, String> groupByMetricIDs) {
		this.definedMetricName = definedMetricName;
		this.groupByMetricIDs = groupByMetricIDs;
	}

	public String getDefinedMetricName() {
		return definedMetricName;
	}

	public Map<String, String> getGroupByMetricIDs() {
		return groupByMetricIDs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((definedMetricName == null) ? 0 : definedMetricName.hashCode());
		result = prime * result + ((groupByMetricIDs == null) ? 0 : groupByMetricIDs.hashCode());
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
		DefinedMetricID other = (DefinedMetricID) obj;
		if (definedMetricName == null) {
			if (other.definedMetricName != null)
				return false;
		} else if (!definedMetricName.equals(other.definedMetricName))
			return false;
		if (groupByMetricIDs == null) {
			if (other.groupByMetricIDs != null)
				return false;
		} else if (!groupByMetricIDs.equals(other.groupByMetricIDs))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DefinedMetricID [definedMetricName=" + definedMetricName + ", groupByMetricIDs=" + groupByMetricIDs + "]";
	}
	
}
