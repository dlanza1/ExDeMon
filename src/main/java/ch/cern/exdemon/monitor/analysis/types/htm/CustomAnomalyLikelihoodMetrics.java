package ch.cern.exdemon.monitor.analysis.types.htm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.numenta.nupic.algorithms.MovingAverage;
import org.numenta.nupic.algorithms.Sample;
import org.numenta.nupic.algorithms.Statistic;
import org.numenta.nupic.algorithms.Anomaly.AveragedAnomalyRecordList;
import org.numenta.nupic.model.Persistable;

import ch.cern.exdemon.monitor.analysis.types.htm.CustomAnomalyLikelihood.AnomalyParams;

public class CustomAnomalyLikelihoodMetrics implements Persistable {
    
	private static final long serialVersionUID = 5310203201494495216L;
	private AnomalyParams params;
    private AveragedAnomalyRecordList aggRecordList;
    private double[] likelihoods;
    
    /**
     * Constructs a new {@code AnomalyLikelihoodMetrics}
     * 
     * @param likelihoods       array of pre-computed estimations
     * @param aggRecordList     List of {@link Sample}s which are basically a set of date, value, average score,
     *                          a list of historical values, and a running total.
     * @param params            {@link AnomalyParams} which are a {@link Statistic}, array of likelihoods,
     *                          and a {@link MovingAverage} 
     */
    public CustomAnomalyLikelihoodMetrics(double[] likelihoods, AveragedAnomalyRecordList aggRecordList, AnomalyParams params) {
        this.params = params;
        this.aggRecordList = aggRecordList;
        this.likelihoods = likelihoods;
    }
    
    /**
     * Utility method to copy this {@link AnomalyLikelihoodMetrics} object.
     * @return
     */
    public CustomAnomalyLikelihoodMetrics copy() {
        List<Object> vals = new ArrayList<Object>();
        for(String key : params.keys()) {
            vals.add(params.get(key));
        }
        
        return new CustomAnomalyLikelihoodMetrics(
            Arrays.copyOf(likelihoods, likelihoods.length), 
            aggRecordList, 
            new AnomalyParams(params.keys(), vals.toArray()));
    }
    
    /**
     * Returns the array of computed likelihoods
     * @return
     */
    public double[] getLikelihoods() {
        return likelihoods;
    }
    
    /**
     * <pre>
     * Returns the record list which are:
     *     List of {@link Sample}s which are basically a set of date, value, average score,
     *     a list of historical values, and a running total.
     * </pre>
     * @return
     */
    public AveragedAnomalyRecordList getAvgRecordList() {
        return aggRecordList;
    }
    
    /**
     * <pre>
     * Returns the {@link AnomalyParams} which is:
     *     a {@link Statistic}, array of likelihoods,
     *     and a {@link MovingAverage}
     * </pre> 
     * @return
     */
    public AnomalyParams getParams() {
        return params;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aggRecordList == null) ? 0 : aggRecordList.hashCode());
        result = prime * result + Arrays.hashCode(likelihoods);
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        CustomAnomalyLikelihoodMetrics other = (CustomAnomalyLikelihoodMetrics)obj;
        if(aggRecordList == null) {
            if(other.aggRecordList != null)
                return false;
        } else if(!aggRecordList.equals(other.aggRecordList))
            return false;
        if(!Arrays.equals(likelihoods, other.likelihoods))
            return false;
        if(params == null) {
            if(other.params != null)
                return false;
        } else if(!params.equals(other.params))
            return false;
        return true;
    }
}
