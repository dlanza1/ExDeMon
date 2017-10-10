package ch.cern.spark.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.spark.TimeUtils;
import ch.cern.spark.metrics.store.Store;

public class ValueHistory implements Serializable {

    private static final long serialVersionUID = 9141577304066319408L;

    private List<DatedValue> values;
    
    private Duration period;

    public ValueHistory(Duration period){
        this.values = new LinkedList<>();
        
        this.period = period;
    }
    
    public void setPeriod(Duration period) {
        this.period = period;
    }

    public void add(Instant time, Float value) {
        values.add(new DatedValue(time, value));
    }
    
    public void purge(Instant time) {
    		Instant oldest_time = time.minus(period);
        
        values = values.stream()
        		.filter(value -> value.getInstant().isAfter(oldest_time))
        		.collect(Collectors.toList());
    }

    public int size() {
        return values.size();
    }

    public List<DatedValue> getDatedValues() {
        return values;
    }

    @Override
    public String toString() {
        return "ValueHistory [metrics=" + values + ", period=" + period + "]";
    }

    public List<Float> getHourlyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    	
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, false))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

	public List<Float> getDaylyValues(Instant time) {
		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

    public List<Float> getWeeklyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), true, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }
    
    private boolean isSameMinute(LocalDateTime time1, LocalDateTime time2, boolean week, boolean day) {
		return time1.get(ChronoField.MINUTE_OF_HOUR) == time2.get(ChronoField.MINUTE_OF_HOUR)
				&& (!day || time1.get(ChronoField.HOUR_OF_DAY) == time2.get(ChronoField.HOUR_OF_DAY))
				&& (!week || time1.get(ChronoField.DAY_OF_WEEK) == time2.get(ChronoField.DAY_OF_WEEK));
	}

    public DescriptiveStatistics getStatistics() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        for (DatedValue value : values)
            stats.addValue(value.getValue());
        
        return stats;
    }
    
    public static class Store_ implements Store{
        private static final long serialVersionUID = 8818532585911816073L;
        
        public ValueHistory history;
        
        private void writeObject(ObjectOutputStream out) throws IOException{
            List<DatedValue> datedValues = history.getDatedValues();
            
            long period = history.getPeriod().getSeconds();
            
            int[] times = new int[datedValues.size()];
            float[] values = new float[datedValues.size()];
            
            int i = 0;
            for (DatedValue value : datedValues) {
                times[i] = (int) value.getInstant().getEpochSecond();
                values[i] = value.getValue();
                
                i++;
            }
            
            out.writeLong(period);
            out.writeObject(times);
            out.writeObject(values);
        }
        
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
            long period = in.readLong();
            
            int[] times = (int[]) in.readObject();
            float[] values = (float[]) in.readObject();
            
            LinkedList<DatedValue> datedValues = new LinkedList<>();
            for (int i = 0; i < times.length; i++)
            		datedValues.add(new DatedValue(Instant.ofEpochSecond(times[i]), values[i]));
            
            history = new ValueHistory(Duration.ofSeconds(period));
            history.setDatedValues(datedValues);
        }
        
    }

    public void reset() {
        this.values = new LinkedList<>();
    }

    public void setDatedValues(LinkedList<DatedValue> newValues) {
        this.values = newValues;
    }

    public Duration getPeriod() {
        return period;
    }
    
}
