package ch.cern.spark.metrics.notifications.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.Template;

public class NotificationsSinkTest {

    @Test
    public void filter() throws ConfigurationException {
        TestNotificationsSink sink = new TestNotificationsSink();

        assertFalse(sink.shouldBeSink(new Notification(null, null, null, null, null, new HashSet<>(Arrays.asList("aa", "bb")))));
        assertTrue(sink.shouldBeSink(new Notification(null, null, null, null, null, new HashSet<>(Arrays.asList("ALL")))));
        assertTrue(sink.shouldBeSink(new Notification(null, null, null, null, null, new HashSet<>(Arrays.asList("test")))));
        assertTrue(sink.shouldBeSink(new Notification(null, null, null, null, null, new HashSet<>(Arrays.asList("aa", "test")))));
        assertTrue(sink.shouldBeSink(new Notification(null, null, null, null, null, new HashSet<>(Arrays.asList("aa", "ALL")))));
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
            result = Template.apply(template, notification);
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
        assertNull(Template.apply(null, null));
        
        String template = "<monitor_id> <notificator_id> <metric_attributes> <metric_attributes:a> <datetime> <reason> <tags> <tags:b>";
        
        assertEquals("null null (empty) null null null (empty) null", Template.apply(template, null));

        Notification notification = new Notification();

        String result = null;
        try {
            result = Template.apply(template, notification);
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
