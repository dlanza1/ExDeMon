package ch.cern.spark.metrics.notifications.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;

public class NotificationsSinkTest extends StreamTestHelper<Notification, Notification> {

    private static final long serialVersionUID = -636788928921418430L;

    @Test
    public void filter() throws ConfigurationException {
        TestNotificationsSink sink = new TestNotificationsSink();

        Monitors.getCache().reset();
        Properties.initCache(null);
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
        properties.setProperty("monitor.mon1.filter.expr", "does not work");
        properties.setProperty("monitor.mon1.analysis.type", "true");
        properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
        properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);

        Set<String> sinks0 = new HashSet<>(Arrays.asList("aa", "bb"));
        addInput(0, new Notification(null, null, null, null, null, sinks0));
        Set<String> sinks1 = new HashSet<>(Arrays.asList("ALL"));
        addInput(0, new Notification(null, null, null, null, null, sinks1));
        Set<String> sinks2 = new HashSet<>(Arrays.asList("test"));
        addInput(0, new Notification(null, null, null, null, null, sinks2));
        Set<String> sinks3 = new HashSet<>(Arrays.asList("aa", "test"));
        addInput(0, new Notification(null, null, null, null, null, sinks3));
        Set<String> sinks4 = new HashSet<>(Arrays.asList("aa", "ALL"));
        addInput(0, new Notification(null, null, null, null, null, sinks4));
        JavaDStream<Notification> notificationsStream = createStream(Notification.class);

        sink.sink(notificationsStream);

        start();
        List<Notification> notifications = sink.notificationsCollector;

        assertEquals(4, notifications.size());
        assertEquals(sinks1, notifications.get(0).getSink_ids());
        assertEquals(sinks2, notifications.get(1).getSink_ids());
        assertEquals(sinks3, notifications.get(2).getSink_ids());
        assertEquals(sinks4, notifications.get(3).getSink_ids());
    }
    
    @Test
    public void template() {
        String template = "<monitor_id> <notificator_id> <metric_attributes> <metric_attributes:a> <datetime> <reason> <tags> <tags:b>";

        Notification notification = new Notification();
        notification.setMonitor_id("M_ID");
        notification.setNotificator_id("N_ID");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "a1");
        metric_attributes.put("b", "b2");
        notification.setMetric_attributes(metric_attributes );
        notification.setNotification_timestamp(Instant.parse("2007-12-03T10:15:30.00Z"));
        notification.setReason("reason_notif");
        Map<String, String> tags = new HashMap<>();
        tags.put("a", "a_tag");
        tags.put("b", "b_tag");
        notification.setTags(tags );
        
        String result = null;
        try {
            result = NotificationsSink.template(template, notification);
        } catch (Exception e) {
            fail();
        }

        assertEquals("M_ID "
                + "N_ID " 
                + "\n\ta = a1" 
                + "\n\tb = b2 "
                + "a1 "
                + "2007-12-03T10:15:30Z "
                + "reason_notif " 
                + "\n\ta = a_tag" 
                + "\n\tb = b_tag "
                + "b_tag", result);
    }

    @Test
    public void nullsInTemplate() {
        assertNull(NotificationsSink.template(null, null));
        
        String template = "<monitor_id> <notificator_id> <metric_attributes> <metric_attributes:a> <datetime> <reason> <tags> <tags:b>";
        
        assertEquals("null null (empty) null null null (empty) null", NotificationsSink.template(template, null));

        Notification notification = new Notification();

        String result = null;
        try {
            result = NotificationsSink.template(template, notification);
        } catch (Exception e) {
            fail();
        }

        assertEquals("null null (empty) null null null (empty) null", result);
    }

    public static class TestNotificationsSink extends NotificationsSink {

        private static final long serialVersionUID = 1281273704318553809L;

        public List<Notification> notificationsCollector = new LinkedList<>();

        public TestNotificationsSink() {
            setId("test");
        }

        @Override
        protected void notify(JavaDStream<Notification> notifications) {
            notifications.foreachRDD(rdd -> notificationsCollector.addAll(rdd.collect()));
        }

    }

}
