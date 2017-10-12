package ch.cern.spark.metrics.notifications;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.store.Store;
import ch.cern.spark.metrics.store.TestStore;
import scala.Tuple2;

public class NotificationStoresRDDTest {
	
	@Test
    public void saveAndLoad() throws ClassNotFoundException, IOException{
    		String path = "/tmp/checkpoint-testing/";
    	
    		List<Tuple2<NotificatorID, Store>> expectedNotifications = new LinkedList<>();
    		NotificatorID id = new NotificatorID("moni1", "notif1", new HashMap<>());
		Store store = new TestStore(1);
		expectedNotifications.add(new Tuple2<NotificatorID, Store>(id, store));
		
		id = new NotificatorID("moni2", "notif2", new HashMap<>());
		store = new TestStore(2);
		expectedNotifications.add(new Tuple2<NotificatorID, Store>(id, store));

		NotificationStoresRDD.save(path, expectedNotifications);
    		
		List<Tuple2<NotificatorID, Store>> loadedNotifications = NotificationStoresRDD.load(path);

		assertEquals(expectedNotifications, loadedNotifications);
    }

}
