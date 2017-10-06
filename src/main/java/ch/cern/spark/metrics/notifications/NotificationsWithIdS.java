package ch.cern.spark.metrics.notifications;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Stream;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.store.Store;

public class NotificationsWithIdS extends Stream<JavaPairDStream<NotificatorID, Store>>  {

    private static final long serialVersionUID = 4249289363250806775L;
    
    public NotificationsWithIdS(JavaPairDStream<NotificatorID, Store> stream) {
        super(stream);
    }

    public void save(final String storing_path) {        
        stream().foreachRDD(new VoidFunction2<JavaPairRDD<NotificatorID, Store>, Time>() {
            private static final long serialVersionUID = -4071427105537124717L;

            @Override
            public void call(JavaPairRDD<NotificatorID, Store> rdd, Time time) throws Exception {
                NotificationStoresRDD stores = new NotificationStoresRDD(rdd);
                
                stores.save(storing_path);
            }
        });
    }
    
}
