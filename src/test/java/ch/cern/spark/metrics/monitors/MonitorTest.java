package ch.cern.spark.metrics.monitors;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.properties.source.StaticPropertiesSource;
import ch.cern.spark.metrics.ComputeIDsForMetricsF;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.UpdateMetricStatusesF;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.ComputeIDsForAnalysisF;
import ch.cern.spark.metrics.store.Store;
import scala.Tuple2;

public class MonitorTest {
	
	Properties propertiesSource = new Properties();
	{
		propertiesSource.put("type", "static");
	}

	ComputeIDsForMetricsF computeIDsForMetricsF = new ComputeIDsForMetricsF(propertiesSource);
	UpdateMetricStatusesF updateMetricStatusesF = new UpdateMetricStatusesF(propertiesSource);
	ComputeIDsForAnalysisF computeIDsForAnalysisF = new ComputeIDsForAnalysisF(propertiesSource);
	UpdateNotificationStatusesF updateNotificationStatusesF = new UpdateNotificationStatusesF(propertiesSource);
	
	State<Store> storeState = null;
	State<Store> notificatorState = null;
	
	@Test
	public void shouldUpdateMonitors() throws Exception {
		Monitors.initCache(null);
		Cache<Map<String, Monitor>> monitorsCache = Monitors.getCache();
		
		Properties.getCache().reset();
		monitorsCache.setExpiration(Duration.ofSeconds(1));
		
		//First load
		Map<String, Monitor> originalMonitors = monitorsCache.get();
		
		Map<String, Monitor> returnedMonitors = monitorsCache.get();
		assertSame(originalMonitors, returnedMonitors);
		
		Thread.sleep(Duration.ofMillis(100).toMillis());
		
		returnedMonitors = monitorsCache.get();
		assertSame(originalMonitors, returnedMonitors);
		
		Thread.sleep(Duration.ofSeconds(1).toMillis());
		
		returnedMonitors = monitorsCache.get();
		assertNotSame(originalMonitors, returnedMonitors);
	}

	@Test
	public void tagsShouldBePropagated() throws Exception {
		
		Properties properties = new Properties();
		properties.setProperty("analysis.type", "fixed-threshold");
		properties.setProperty("analysis.error.upperbound", "20");
		properties.setProperty("analysis.error.lowerbound", "10");
		properties.setProperty("tags.email", "1234@cern.ch");
		properties.setProperty("tags.group", "IT_DB");
		properties.setProperty("tags.group", "IT_DB");
		Monitor monitor = new Monitor("test").config(properties);
		
		State<Store> store = new StateImpl<>();
		
		AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>())).get();
		assertEquals(AnalysisResult.Status.ERROR, result.getStatus());
		assertEquals("1234@cern.ch", result.getTags().get("email"));
		assertEquals("IT_DB", result.getTags().get("group"));
		
		result = monitor.process(store, new Metric(Instant.now(), 15f, new HashMap<>())).get();
		assertEquals(AnalysisResult.Status.OK, result.getStatus());
		assertEquals("1234@cern.ch", result.getTags().get("email"));
		assertEquals("IT_DB", result.getTags().get("group"));
	}
	
	@Test
	public void shouldProduceExceptionAnaylisisResultWithConfigurationExceptionAndFilterOK() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "HOST=host1");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
		StaticPropertiesSource.properties = properties;
		Properties.resetCache();
		Monitors.getCache().reset();
		
		storeState = new StateImpl<>();
		notificatorState = new StateImpl<>();
		
		Optional<AnalysisResult> result = getAnalysisResult(Metric(0, 0, "HOST=host1"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(20, 0, "HOST=host2345"));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(40, 0, "HOST=host1"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(60, 0, "HOST=host243"));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(80, 0));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(100, 0, "HOST=host1"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(120, 0, "HOST=host1"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(140, 0));
		assertFalse(result.isPresent());
	}
	
	@Test
	public void shouldProducePeriodicNotificationsWithConfigurationExceptionAndFilterOK() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "HOST=host1");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
		StaticPropertiesSource.properties = properties;
		Properties.resetCache();
		Monitors.getCache().reset();
		
		storeState = new StateImpl<>();
		notificatorState = new StateImpl<>();
		
		Instant time = Instant.now();
		Optional<Notification> notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertFalse(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host2"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertTrue(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host2"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertFalse(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host2"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertFalse(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host2"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertTrue(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host2"));
		assertFalse(notification.isPresent());
	}

	@Test
	public void shouldProducePeriodicExceptionAnaylisisResultWithFilterConfigurationException() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "does not work");
		properties.setProperty("monitor.mon1.analysis.type", "true");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
		StaticPropertiesSource.properties = properties;
		Properties.resetCache();
		Monitors.getCache().reset();
		
		storeState = new StateImpl<>();
		notificatorState = new StateImpl<>();
		
		Optional<AnalysisResult> result = getAnalysisResult(Metric(0, 0, "HOST=host1"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(20, 0, "HOST=host2345"));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(40, 0, "HOST=host3454"));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(60, 0, "HOST=host243"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(80, 0));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(100, 0, "HOST=host5426"));
		assertFalse(result.isPresent());
		
		result = getAnalysisResult(Metric(120, 0, "HOST=ho"));
		assertTrue(result.isPresent());
		
		result = getAnalysisResult(Metric(140, 0));
		assertFalse(result.isPresent());
	}
	
	@Test
	public void shouldProducePeriodicNotificationsWithFilterConfigurationException() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "does not work");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
		StaticPropertiesSource.properties = properties;
		Properties.resetCache();
		Monitors.getCache().reset();
		
		storeState = new StateImpl<>();
		notificatorState = new StateImpl<>();
		
		Instant time = Instant.now();
		Optional<Notification> notification = getNotification(Metric(time, 0, "HOST=host1"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=hos4324"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host3542"));
		assertTrue(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=host34523"));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=hos43"));
		assertFalse(notification.isPresent());
		notification = getNotification(Metric(time, 0, "HOST=ho12342"));
		assertFalse(notification.isPresent());
		notification = getNotification(Metric(time, 0));
		assertFalse(notification.isPresent());
		
		time = time.plus(Duration.ofMinutes(20));
		notification = getNotification(Metric(time, 0, "HOST=host34234"));
		assertTrue(notification.isPresent());
	}
	
	private Optional<AnalysisResult> getAnalysisResult(Metric metric) throws Exception {
		Iterator<Tuple2<MonitorIDMetricIDs, Metric>> metricIds = computeIDsForMetricsF.call(metric);
		if(!metricIds.hasNext())
			return Optional.empty();
		
		return updateMetricStatusesF.call(new Time(metric.getInstant().toEpochMilli()), metricIds.next()._1, Optional.of(metric), storeState);
	}
	
	private Optional<Notification> getNotification(Metric metric) throws Exception {
		Optional<AnalysisResult> result = getAnalysisResult(metric);
		if(!result.isPresent())
			return Optional.empty();
		
		Iterator<Tuple2<NotificatorID, AnalysisResult>> analysisIds = computeIDsForAnalysisF.call(result.get());
		analysisIds.hasNext();
		if(!analysisIds.hasNext())
			return Optional.empty();
		
		return updateNotificationStatusesF.call(new Time(metric.getInstant().toEpochMilli()), analysisIds.next()._1, result, notificatorState);
	}

}
