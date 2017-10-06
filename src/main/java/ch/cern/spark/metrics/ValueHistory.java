package ch.cern.spark.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.spark.metrics.store.Store;

public class ValueHistory implements Serializable {

    private static final long serialVersionUID = 9141577304066319408L;

    private LinkedList<DatedValue> values;
    
    private long period_in_seconds;

    private Calendar calendar;    

    public ValueHistory(long period_in_seconds){
        this.values = new LinkedList<>();
        
        this.period_in_seconds = period_in_seconds;
        
        calendar = GregorianCalendar.getInstance();
    }
    
    public void setPeriod(long period_in_seconds) {
        this.period_in_seconds = period_in_seconds;
    }

    public void add(Date time, Float value) {
        int timeInSeoncds = (int) (time.getTime() / 1000);
        
        values.add(new DatedValue(timeInSeoncds, value));

        Collections.sort(values, getComparator());
    }
    
    public void removeRecordsOutOfPeriodForTime(Date time) {
        Iterator<DatedValue> iter = values.iterator();
        
        long time_in_seconds = time.getTime() / 1000;
        
        long oldest_time_in_seconds = time_in_seconds - period_in_seconds;
        
        while(iter.hasNext()){
            long metric_time_in_seconds = iter.next().getTimeInSeconds();
            
            if(metric_time_in_seconds < oldest_time_in_seconds){
                iter.remove();
            }
        }
    }

    private Comparator<DatedValue> getComparator() {
        return new Comparator<DatedValue>() {
            @Override
            public int compare(DatedValue o1, DatedValue o2) {
                return Integer.compare(o1.getTimeInSeconds(), o2.getTimeInSeconds());
            }
        };
    }

    public int size() {
        return values.size();
    }

    public LinkedList<DatedValue> getDatedValues() {
        return values;
    }

    @Override
    public String toString() {
        return "ValueHistory [metrics=" + values + ", period_in_seconds=" + period_in_seconds + "]";
    }

    public List<Float> getHourlyValues(Date timestamp) {
        List<Float> values = new LinkedList<>();
        calendar.setTime(timestamp); 

        int requested_minute = calendar.get(Calendar.MINUTE);
        
        for (DatedValue value: this.values) {
            calendar.setTime(value.getDate());
            
            int metric_minute = calendar.get(Calendar.MINUTE);
            
            if(requested_minute == metric_minute)
                values.add(value.getValue());
        }
        
        return values;
    }

    public List<Float> getDaylyValues(Date timestamp) {
        List<Float> values = new LinkedList<>();
        calendar.setTime(timestamp); 

        int requested_minute = calendar.get(Calendar.MINUTE);
        int requested_hour = calendar.get(Calendar.HOUR_OF_DAY);
        
        for (DatedValue value: this.values) {
            calendar.setTime(value.getDate());
            
            int metric_minute = calendar.get(Calendar.MINUTE);
            int metric_hour = calendar.get(Calendar.HOUR_OF_DAY);
            
            if(requested_minute == metric_minute && requested_hour == metric_hour)
                values.add(value.getValue());
        }
        
        return values;
    }

    public List<Float> getWeeklyValues(Date timestamp) {
        List<Float> values = new LinkedList<>();
        calendar.setTime(timestamp); 

        int requested_minute = calendar.get(Calendar.MINUTE);
        int requested_hour = calendar.get(Calendar.HOUR_OF_DAY);
        int requested_day_of_week = calendar.get(Calendar.DAY_OF_WEEK);
        
        for (DatedValue value: this.values) {
            calendar.setTime(value.getDate());
            
            int metric_minute = calendar.get(Calendar.MINUTE);
            int metric_hour = calendar.get(Calendar.HOUR_OF_DAY);
            int metric_day_of_week = calendar.get(Calendar.DAY_OF_WEEK);
            
            if(requested_minute == metric_minute 
                    && requested_hour == metric_hour
                    && requested_day_of_week == metric_day_of_week)
                values.add(value.getValue());
        }
        
        return values;
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
            LinkedList<DatedValue> datedValues = history.getDatedValues();
            
            long period = history.getPeriod();
            
            int[] times = new int[datedValues.size()];
            float[] values = new float[datedValues.size()];
            
            int i = 0;
            for (DatedValue value : datedValues) {
                times[i] = value.getTimeInSeconds();
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
                datedValues.add(new DatedValue(times[i], values[i]));
            
            history = new ValueHistory(period);
            history.setDatedValues(datedValues);
        }
        
    }

    public void reset() {
        this.values = new LinkedList<>();
    }

    public void setDatedValues(LinkedList<DatedValue> newValues) {
        this.values = newValues;
    }

    public long getPeriod() {
        return period_in_seconds;
    }
    
}
