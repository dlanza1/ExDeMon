package ch.cern.spark.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.spark.metrics.defined.equation.ComputationException;
import ch.cern.spark.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.DurationAndTruncate;
import ch.cern.utils.TimeUtils;

public class ValueHistory implements Serializable {

    private static final long serialVersionUID = 9141577304066319408L;
    
    private static final long MAX_SIZE_DEFAULT = 100000;

    private List<DatedValue> values;
    
    private DurationAndTruncate period = new DurationAndTruncate(Duration.ofMinutes(30));

    private long max_size;

    private ChronoUnit granularity;
    private Aggregation aggregation;

    public ValueHistory(Duration expire){
        this(new DurationAndTruncate(expire), MAX_SIZE_DEFAULT, null, null);
    }
    
    public ValueHistory(DurationAndTruncate expire, long max_size, ChronoUnit granularity, Aggregation aggregation){
        this.values = new LinkedList<>();
        
        this.max_size = max_size;
        
        this.max_size = max_size;
        this.period = expire;
        this.granularity = granularity;
        this.aggregation = aggregation;
    }
    
    public void setPeriod(DurationAndTruncate period) {
        this.period = period;
    }

    public void add(Instant time, float value) {
        add(time, new FloatValue(value));
    }
    
    public void add(Instant time, Value value) {
        if(values.size() >= (max_size * 0.9))
            summarizeValues(time);

        // Removing the oldest entry if max size
        if (values.size() >= max_size + 1)
            values.remove(values.iterator().next());
        
        values.add(new DatedValue(time, value));
    }
    
    private void summarizeValues(Instant time) {
        if(granularity == null || aggregation == null)
            return;
  
        Map<Instant, List<DatedValue>> groupedValues = values.stream()
                                                        .collect(Collectors.groupingBy(v -> v.getInstant().truncatedTo(granularity)));
        
        values = new LinkedList<>();
        for (Map.Entry<Instant, List<DatedValue>> group : groupedValues.entrySet())
            values.add(new DatedValue(group.getKey(), aggregation.aggregateValues(group.getValue(), group.getKey())));
        
        values = values.stream().sorted().collect(Collectors.toList());
    }

    public void purge(Instant time) {
        if(period == null)
            return;
        
    		Instant oldest_time = period.adjust(time);
    		values.removeIf(value -> value.getInstant().isBefore(oldest_time));
    }

    public int size() {
        return values.size();
    }

    public List<DatedValue> getDatedValues() throws ComputationException {
        if(values.size() > max_size)
            throw new ComputationException("Maximum aggregation size reached. You may mitigate that by increasing granularity.");
        
        return values;
    }

    @Override
    public String toString() {
        return "ValueHistory [values=" + values + ", period=" + period + ", max_size=" + max_size + ", granularity="
                + granularity + ", aggregation=" + aggregation + "]";
    }

    public List<Value> getHourlyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    	
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, false))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

	public List<Value> getDaylyValues(Instant time) {
		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

    public List<Value> getWeeklyValues(Instant time) {
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
        
        values.stream()
        		.map(val -> val.getValue().getAsFloat())
        		.filter(Optional::isPresent).map(Optional::get)
        		.forEach(stats::addValue);
        
        return stats;
    }
    
    @ClassNameAlias("value-history")
    public static class Status extends StatusValue{
        private static final long serialVersionUID = 8818532585911816073L;
        
        public ValueHistory history;
        
        public Status() {
            history = new ValueHistory(Duration.ofMinutes(10));
        }

        public Status(int max_aggregation_size, DurationAndTruncate expire, ChronoUnit granularity, Aggregation aggregation) {
            history = new ValueHistory(expire, max_aggregation_size, granularity, aggregation);
        }

        private void writeObject(ObjectOutputStream out) throws IOException{
            List<DatedValue> datedValues = history.values;
            
            DurationAndTruncate period = history.getPeriod();
            ChronoUnit granularity = history.getGranularity();
            Aggregation agg = history.getAggregation();
            
            int[] times = new int[datedValues.size()];
            Value[] values = new Value[datedValues.size()];
            
            int i = 0;
            for (DatedValue value : datedValues) {
                times[i] = (int) value.getInstant().getEpochSecond();
                values[i] = value.getValue();
                
                i++;
            }
            
            out.writeObject(period);
            out.writeObject(granularity);
            out.writeObject(agg);
            out.writeObject(times);
            out.writeObject(values);
        }
        
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
            DurationAndTruncate period = (DurationAndTruncate) in.readObject();
            ChronoUnit granularity = (ChronoUnit) in.readObject();
            Aggregation aggregation = (Aggregation) in.readObject();
            history = new ValueHistory(period, MAX_SIZE_DEFAULT, granularity, aggregation);
            
            int[] times = (int[]) in.readObject();
            Value[] values = (Value[]) in.readObject();
            
            List<DatedValue> datedValues = IntStream.range(0, times.length)
								            		.mapToObj(i -> new DatedValue(Instant.ofEpochSecond(times[i]), values[i]))
								            		.collect(Collectors.toList());
            
            history.setDatedValues(datedValues);
        }
        
    }

    public void reset() {
        this.values = new LinkedList<>();
    }

    public void setDatedValues(List<DatedValue> newValues) {
        this.values = newValues;
    }

    public DurationAndTruncate getPeriod() {
        return period;
    }
    
    public ChronoUnit getGranularity() {
        return granularity;
    }
    
    public void setGranularity(ChronoUnit granularity) {
        this.granularity = granularity;
    }
    
    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }
    
    public Aggregation getAggregation() {
        return aggregation;
    }
    
    public void setMax_size(long max_size) {
        this.max_size = max_size;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
        result = prime * result + ((granularity == null) ? 0 : granularity.hashCode());
        result = prime * result + (int) (max_size ^ (max_size >>> 32));
        result = prime * result + ((period == null) ? 0 : period.hashCode());
        result = prime * result + ((values == null) ? 0 : values.hashCode());
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
        ValueHistory other = (ValueHistory) obj;
        if (aggregation == null) {
            if (other.aggregation != null)
                return false;
        } else if (!aggregation.getClass().equals(other.aggregation.getClass()))
            return false;
        if (granularity != other.granularity)
            return false;
        if (max_size != other.max_size)
            return false;
        if (period == null) {
            if (other.period != null)
                return false;
        } else if (!period.equals(other.period))
            return false;
        if (values == null) {
            if (other.values != null)
                return false;
        } else if (!values.equals(other.values))
            return false;
        return true;
    }
    
}
