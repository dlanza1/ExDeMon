package ch.cern.spark.metrics.notifications;

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

import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.store.Store;
import scala.Tuple2;

public class NotificationStoresRDD extends JavaRDD<Tuple2<NotificatorID, Store>> {

    private static final long serialVersionUID = 3059823765482102618L;

    private static transient FileSystem fs = null;
    
    public NotificationStoresRDD(JavaRDD<Tuple2<NotificatorID, Store>> rdd) {
        super(rdd.rdd(), rdd.classTag());
    }

    public static NotificationStoresRDD load(String storing_path, JavaSparkContext context) throws IOException, ClassNotFoundException {
        setFileSystem();
        
        Path file = getStoringFile(storing_path);
        
        if(fs.exists(file)){
            ObjectInputStream is = new ObjectInputStream(fs.open(file));
            
            @SuppressWarnings("unchecked")
            List<Tuple2<NotificatorID, Store>> stores = 
                    (List<Tuple2<NotificatorID, Store>>) is.readObject();
            
            return new NotificationStoresRDD(context.parallelize(stores));
        }else{
            return empty(context);
        }
    }
    
    public static NotificationStoresRDD empty(JavaSparkContext context){
        return new NotificationStoresRDD(context.parallelize(new LinkedList<Tuple2<NotificatorID, Store>>()));
    }

    public void save(String storing_path) throws IOException {
        setFileSystem();
        
        Path finalFile = getStoringFile(storing_path);
        Path tmpFile = finalFile.suffix(".tmp");
        
        fs.mkdirs(tmpFile.getParent());
        
        List<Tuple2<NotificatorID, Store>> metricStores = collect();

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
    
    private static void setFileSystem() throws IOException {
        if(fs == null)
            fs = FileSystem.get(new Configuration());
    }
    
    private static Path getStoringFile(String storing_path){
        return new Path(storing_path + "/notificators/latest");
    }
    
}
