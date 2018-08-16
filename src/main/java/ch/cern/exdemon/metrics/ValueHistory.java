package ch.cern.exdemon.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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

import ch.cern.exdemon.metrics.defined.equation.ComputationException;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.LimitedQueue;
import ch.cern.utils.TimeUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@EqualsAndHashCode(callSuper=false)
@ToString
public class ValueHistory implements Serializable {

    private static final long serialVersionUID = 9141577304066319408L;

    @Getter @Setter
    private List<DatedValue> values;

    @Getter @Setter
    private long max_size;

    @Getter @Setter
    private ChronoUnit granularity;
    
    @Getter @Setter
    private Aggregation aggregation;
    
    @Getter
    private List<Metric> lastAggregatedMetrics;
    @Getter @Setter
    private int max_lastAggregatedMetrics_size = 0;

    public ValueHistory(){
        this(ValueVariable.MAX_SIZE_DEFAULT, 0, null, null);
    }
    
    public ValueHistory(long max_size, int max_lastAggregatedMetrics_size, ChronoUnit granularity, Aggregation aggregation){
        this.values = new LinkedList<>();
        this.max_lastAggregatedMetrics_size = (int) Math.min(max_size, max_lastAggregatedMetrics_size);
        
        this.max_size = max_size;
        this.granularity = granularity;
        this.aggregation = aggregation;
    }

    public void add(Instant time, float value) {
        add(time, new FloatValue(value));
    }

    public void add(Instant time, Value value, Metric originalMetric) {
        addLastAggMetric(originalMetric);
        add(time, value);
    }
    
    private void addLastAggMetric(Metric metric) {
        if(max_lastAggregatedMetrics_size <= 0) {
            lastAggregatedMetrics = null;
            return;
        }
        
        if(lastAggregatedMetrics == null)
            lastAggregatedMetrics = new LimitedQueue<>(max_lastAggregatedMetrics_size);
        
        if(metric != null)
            lastAggregatedMetrics.add(metric);
    }
    
    public void add(Instant time, Value value) {
        if(time == null || value == null)
            return;
        
        if(values.size() >= (max_size * 0.9))
            summarizeValues(time);

        if (isMaxSizeReached())
            values.remove(values.iterator().next());
        
        values.add(new DatedValue(time, value));
    }

	private boolean isMaxSizeReached() {
		return values.size() > max_size;
	}
    
    private void summarizeValues(Instant time) {
        if(granularity == null || aggregation == null)
            return;
  
        Map<Instant, List<DatedValue>> groupedValues = values.stream()
                                                        .collect(Collectors.groupingBy(v -> v.getTime().truncatedTo(granularity)));
        
        values = new LinkedList<>();
        for (Map.Entry<Instant, List<DatedValue>> group : groupedValues.entrySet())
            values.add(new DatedValue(group.getKey(), aggregation.aggregateValues(group.getValue(), group.getKey())));
        
        values = values.stream().sorted().collect(Collectors.toList());
    }

    public void purge(Instant oldest_time) {
    	values.removeIf(value -> value.getTime().isBefore(oldest_time));
    }

    public int size() {
        return values.size();
    }

    public List<DatedValue> getDatedValues() throws ComputationException {
        if(isMaxSizeReached())
            throw new ComputationException("Maximum aggregation size reached. You may mitigate that by increasing granularity.");
        
        return values;
    }

    public List<Value> getHourlyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    	
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getTime()), false, false))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

	public List<Value> getDaylyValues(Instant time) {
		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getTime()), false, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

    public List<Value> getWeeklyValues(Instant time) {
        LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getTime()), true, true))
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
    
    @EqualsAndHashCode(callSuper=true)
    @ToString
    @ClassNameAlias("value-history")
    public static class Status extends StatusValue{
        private static final long serialVersionUID = 8818532585911816073L;
        
        public ValueHistory history;
        
        public Status() {
            history = new ValueHistory();
        }

        public Status(int max_aggregation_size, int max_lastAggregatedMetrics_size, ChronoUnit granularity, Aggregation aggregation) {
            history = new ValueHistory(max_aggregation_size, max_lastAggregatedMetrics_size, granularity, aggregation);
        }

        private void writeObject(ObjectOutputStream out) throws IOException{
            List<DatedValue> datedValues = history.values;
            
            ChronoUnit granularity = history.getGranularity();
            Aggregation agg = history.getAggregation();
            
            int[] times = new int[datedValues.size()];
            Value[] values = new Value[datedValues.size()];
            
            int i = 0;
            for (DatedValue value : datedValues) {
                times[i] = (int) value.getTime().getEpochSecond();
                values[i] = value.getValue();
                
                i++;
            }
            
            out.writeObject(granularity);
            out.writeObject(agg);
            out.writeObject(times);
            out.writeObject(values);
        }
        
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
            ChronoUnit granularity = (ChronoUnit) in.readObject();
            Aggregation aggregation = (Aggregation) in.readObject();
            history = new ValueHistory(ValueVariable.MAX_SIZE_DEFAULT, 0, granularity, aggregation);
            
            int[] times = (int[]) in.readObject();
            Value[] values = (Value[]) in.readObject();
            
            List<DatedValue> datedValues = IntStream.range(0, times.length)
								            		.mapToObj(i -> new DatedValue(Instant.ofEpochSecond(times[i]), values[i]))
								            		.collect(Collectors.toList());
            
            history.setValues(datedValues);
        }
        
    }

    public void reset() {
        this.values = new LinkedList<>();
        
        if(max_lastAggregatedMetrics_size > 0)
            this.lastAggregatedMetrics = new LimitedQueue<>(max_lastAggregatedMetrics_size);
        else
            this.lastAggregatedMetrics = null;
    }

}
