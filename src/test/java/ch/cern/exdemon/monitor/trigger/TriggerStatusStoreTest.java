package ch.cern.exdemon.monitor.trigger;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import ch.cern.spark.status.storage.types.SingleFileStatusesStorage;
import scala.Tuple2;

public class TriggerStatusStoreTest {
	
	private static transient FileSystem fs = null;
	
	private String path;
	
	@Before
	public void setUp() throws Exception {
		path = "/tmp/" + TriggerStatusStoreTest.class.getSimpleName();
		
		setFileSystem();
		fs.delete(new Path(path), true);
	}
	
	@Test
    public void saveAndLoad() throws ClassNotFoundException, IOException, ConfigurationException{
		List<Tuple2<TriggerStatusKey, StatusValue>> expectedNotifications = new LinkedList<>();
		TriggerStatusKey id = new TriggerStatusKey("moni1", "trigger1", new HashMap<>());
		StatusValue store = new TestStatus(1);
		expectedNotifications.add(new Tuple2<TriggerStatusKey, StatusValue>(id, store));
		
		id = new TriggerStatusKey("moni2", "trigger2", new HashMap<>());
		store = new TestStatus(2);
		expectedNotifications.add(new Tuple2<TriggerStatusKey, StatusValue>(id, store));

		SingleFileStatusesStorage storage = new SingleFileStatusesStorage();
		Properties properties = new Properties();
		properties.setProperty("path", path);
		storage.config(properties);
		
		storage.save(expectedNotifications);
    		
		List<Tuple2<StatusKey, StatusValue>> loadedNotifications = storage.load(new Path(path + "/" + TriggerStatusKey.class.getSimpleName() + "/latest"));

		assertEquals(expectedNotifications, loadedNotifications);
    }
	
	private void setFileSystem() throws IOException {
		if (fs == null)
			fs = FileSystem.get(new Configuration());
	}

}
