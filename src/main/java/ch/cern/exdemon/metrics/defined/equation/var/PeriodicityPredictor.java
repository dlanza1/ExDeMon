package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import ch.cern.utils.IntEvictingQueue;

public class PeriodicityPredictor {
	
	private static Median MEDIAN = new Median();
	private static Mean MEAN = new Mean();
	
	private IntEvictingQueue times; 
	
	private int period = -1;
	private int delay = -1;

	public PeriodicityPredictor() {
		times = new IntEvictingQueue(20);
	}
	
	public void add(Instant timeToAdd) {
		times.add((int) timeToAdd.getEpochSecond());
		
		period = -1;
		delay = -1;
	}
	
	public List<Instant> get(Instant fromTime, Duration forPeriod){
		List<Instant> predictedTimes = new LinkedList<>();
		
		Optional<Integer> period = getPeriod();
		if(!period.isPresent())
			return predictedTimes;
		
		Optional<Integer> delay = getDelay();
		if(!delay.isPresent())
			return predictedTimes;
		
		Instant until = fromTime.plus(forPeriod);
		
		int forTimeDelay = (int) (fromTime.getEpochSecond() % period.get());
		
		Instant predictedTime = fromTime.plus(Duration.ofSeconds(delay.get() - forTimeDelay));
		for (int i = 0; i < 100; i++) {
			if(predictedTime.compareTo(until) < 0){
				if(predictedTime.compareTo(fromTime) >= 0)
					predictedTimes.add(predictedTime);
			}else
				return predictedTimes;
			
			predictedTime = predictedTime.plus(Duration.ofSeconds(period.get()));
		}
		
		return predictedTimes;
	}

	public Optional<Integer> getPeriod() {
		if(this.period != -1)
			return Optional.of(this.period);
		
		double[] periods = getPeriods();
		if(periods.length == 0)
			return Optional.empty();

		synchronized(MEDIAN) {
			this.period = (int) MEDIAN.evaluate(periods);
		}
		
		if(this.period == 0)
			this.period = 1;
		
		return Optional.of(this.period);
	}

	private double[] getPeriods() {
		if(times.size() < 2)
			return new double[0];
		
		double[] periods = new double[times.size() - 1];
		
		for (int i = 1; i < times.size(); i++)
			periods[i - 1] = Math.abs(times.get(i) - times.get(i -1));
			
		return periods;
	}
	
	public Optional<Integer> getDelay() {
		if(this.delay != -1)
			return Optional.of(this.delay);
		
		double[] delays = getDelays();
		if(delays.length == 0)
			return Optional.empty();

		synchronized(MEAN) {
			this.delay = (int) MEAN.evaluate(delays);
		}
		
		return Optional.of(this.delay);
	}
	
	private double[] getDelays() {
		if(times.size() < 2)
			return new double[0];
		
		getPeriod();
		
		double[] delays = new double[times.size()];
		
		for (int i = 0; i < times.size(); i++)
			delays[i] = times.get(i) % period;
		
		return delays;
	}
	
}
