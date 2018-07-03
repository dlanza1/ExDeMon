package ch.cern.exdemon.metrics.defined;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ClassNameAlias("defined-metric-key")
@ToString
@EqualsAndHashCode(callSuper=false)
public class DefinedMetricStatuskey implements IDStatusKey {

	private static final long serialVersionUID = 2992810464329306975L;

	private String id;
	
	@Getter
	private Map<String, String> metric_attributes;

	public DefinedMetricStatuskey(String definedMetricId, Map<String, String> metric_attributes) {
		this.id = definedMetricId;
		this.metric_attributes = metric_attributes;
	}

	public String getID() {
		return id;
	}
	
}
