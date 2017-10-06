package ch.cern.spark.metrics.preanalysis.types;

import java.util.Date;

import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class DifferencePreAnalysis extends PreAnalysis implements HasStore {

    private static final long serialVersionUID = -7637460204263005199L;

    private float previousValue = Float.NaN;
    
    public DifferencePreAnalysis() {
        super(DifferencePreAnalysis.class, "difference");
    }
    
    @Override
    public float process(Date metric_timestamp, float metric_value) {
        float prePreviousValue = previousValue;
        
        previousValue = metric_value;
        
        if(!Float.isNaN(prePreviousValue))
            return metric_value - prePreviousValue;
        else
            return 0;
    }
    
    @Override
    public void load(Store store) {
        if(store instanceof Store_){
            previousValue = ((Store_) store).previousValue;
        }
    }

    @Override
    public Store save() {
        Store_ store = new Store_();
        
        store.previousValue = previousValue;
        
        return store;
    }
    
    public static class Store_ implements Store{
        private static final long serialVersionUID = 101968781882733133L;
        
        private float previousValue;
    }

}
