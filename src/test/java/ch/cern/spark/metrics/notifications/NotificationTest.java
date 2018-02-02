package ch.cern.spark.metrics.notifications;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

public class NotificationTest {
    
    public static Notification DUMMY = new Notification(
                                                        Instant.now(), 
                                                        "dummyMonitorID", 
                                                        "dummyNotificatorID",
                                                        new HashMap<>(),
                                                        "dummy reason",
                                                        new HashSet<>(),
                                                        new HashMap<>());

}
