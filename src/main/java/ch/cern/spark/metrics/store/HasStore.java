package ch.cern.spark.metrics.store;

public interface HasStore {

    public void load(Store store);
    
    public Store save();

}
