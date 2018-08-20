package ch.cern.exdemon.monitor.analysis.types.htm;

import java.io.Serializable;

public class AnomaliesResults implements Serializable{
	
	private static final long serialVersionUID = 2504261847208235950L;
	private double score;
	private double errThreshold;
	private double warnThreshold;
	
	public AnomaliesResults(double score, double errorThreshold, double warningThreshold){
		setAnomalyLikelihoodScore(score);
		setErrThreshold(errorThreshold);
		setWarnThreshold(warningThreshold);
	}

	public double getAnomalyLikelihoodScore() {
		return score;
	}

	public void setAnomalyLikelihoodScore(double score) {
		this.score = score;
	}
	
	public double getErrThreshold() {
		return errThreshold;
	}

	public void setErrThreshold(double errThreshold) {
		this.errThreshold = errThreshold;
	}

	public double getWarnThreshold() {
		return warnThreshold;
	}

	public void setWarnThreshold(double warnThreshold) {
		this.warnThreshold = warnThreshold;
	}
	
	public boolean isError() {
		return score > getErrThreshold(); 
	}
	
	public boolean isWarning() {
		return score > getWarnThreshold();
	}
}
