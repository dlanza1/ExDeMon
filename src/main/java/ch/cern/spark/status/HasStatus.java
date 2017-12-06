package ch.cern.spark.status;

public interface HasStatus {

    public void load(StatusValue store);
    
    public StatusValue save();

}
