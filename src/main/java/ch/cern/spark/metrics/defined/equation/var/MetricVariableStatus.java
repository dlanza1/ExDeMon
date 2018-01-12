package ch.cern.spark.metrics.defined.equation.var;

import java.util.List;
import java.util.Map;

import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;

@Deprecated
@SuppressWarnings("unused")
@ClassNameAlias("metric-variable")
public class MetricVariableStatus extends StatusValue {

    private static final long serialVersionUID = -7439047274576894171L;

    public static final int MAX_AGGREGATION_SIZE = 100000;

    private Map<Integer, DatedValue> aggregationValues;
    private List<DatedValue> alongTimeValues;

}