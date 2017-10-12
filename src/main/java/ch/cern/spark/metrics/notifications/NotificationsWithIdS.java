package ch.cern.spark.metrics.notifications;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.store.Store;
import scala.Tuple2;

public class NotificationsWithIdS extends JavaDStream<Tuple2<NotificatorID, Store>>  {

    private static final long serialVersionUID = 4249289363250806775L;
    
    public NotificationsWithIdS(JavaDStream<Tuple2<NotificatorID, Store>> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public void save(String storing_path) {     
    		foreachRDD(rdd -> new NotificationStoresRDD(rdd).save(storing_path));
    }
    
}
