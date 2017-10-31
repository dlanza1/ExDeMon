package ch.cern.spark;

import ch.cern.properties.Properties;

public class SparkConf extends org.apache.spark.SparkConf {

    private static final long serialVersionUID = 2168292945784881986L;

    public SparkConf(){
        super();
    }

    public void runLocallyIfMasterIsNotConfigured() {
        if(!contains("spark.master")) {
            setMaster("local[1]");
            set("spark.driver.host", "localhost");
        }
    }

    public void addProperties(Properties properties, String prefix) {
    		properties.stringPropertyNames().stream()
    			.filter(key -> key.startsWith(prefix))
    			.forEach(key -> set(key, properties.getProperty(key)));
    }
    
}
