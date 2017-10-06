package ch.cern.spark;

public class SparkConf extends org.apache.spark.SparkConf {

    private static final long serialVersionUID = 2168292945784881986L;

    public SparkConf(){
        super();
    }

    public void runLocallyIfMasterIsNotConfigured() {
        if(!contains("spark.master"))
            setMaster("local[1]");
    }

    public void addProperties(Properties properties, String prefix) {
        for(String key : properties.stringPropertyNames())
            if(key.startsWith(prefix))
                set(key, properties.getProperty(key));
    }
    
}
