package ch.cern.spark.metrics.store;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ch.cern.spark.metrics.MonitorIDMetricIDs;
import scala.Tuple2;

public class MetricStoresRDD extends JavaRDD<Tuple2<MonitorIDMetricIDs, MetricStore>> {

    private static final long serialVersionUID = 3741287858945087706L;
    
    private static transient FileSystem fs = null;
    
    public MetricStoresRDD(JavaRDD<Tuple2<MonitorIDMetricIDs, MetricStore>> rdd) {
        super(rdd.rdd(), rdd.classTag());
    }
    
    public void save(String storing_path) throws IllegalArgumentException, IOException {
        setFileSystem();
        
        Path finalFile = getStoringFile(storing_path);
        Path tmpFile = finalFile.suffix(".tmp");
        
        fs.mkdirs(tmpFile.getParent());
        
        List<Tuple2<MonitorIDMetricIDs, MetricStore>> metricStores = collect();

        ObjectOutputStream oos = new ObjectOutputStream(fs.create(tmpFile, true));
        oos.writeObject(metricStores);
        oos.close();
        
        if(canBeRead(tmpFile)){
            fs.delete(finalFile, false);
            fs.rename(tmpFile, finalFile);
        }
            
    }
    
    private boolean canBeRead(Path tmpFile) throws IOException {
        setFileSystem();
        
        try {
            ObjectInputStream is = new ObjectInputStream(fs.open(tmpFile));
            is.readObject();
            is.close();
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public static MetricStoresRDD load(String storing_path, JavaSparkContext context) throws IOException, ClassNotFoundException {
        setFileSystem();
        
        Path file = getStoringFile(storing_path);
        
        if(fs.exists(file)){
            ObjectInputStream is = new ObjectInputStream(fs.open(file));
            
            List<Tuple2<MonitorIDMetricIDs, MetricStore>> stores = 
                    (List<Tuple2<MonitorIDMetricIDs, MetricStore>>) is.readObject();
            
            return new MetricStoresRDD(context.parallelize(stores));
        }else{
            return empty(context);
        }
    }
    
    public static MetricStoresRDD empty(JavaSparkContext context){
        return new MetricStoresRDD(context.parallelize(new LinkedList<Tuple2<MonitorIDMetricIDs, MetricStore>>()));
    }
    
    private static void setFileSystem() throws IOException {
        if(fs == null)
            fs = FileSystem.get(new Configuration());
    }
    
    private static Path getStoringFile(String storing_path){
        return new Path(storing_path + "/metricStores/latest");
    }

}
