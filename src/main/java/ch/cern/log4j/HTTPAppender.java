package ch.cern.log4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import ch.cern.properties.Properties;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.http.JsonPOSTRequest;

public class HTTPAppender extends AppenderSkeleton {

    private static final BlockingQueue<LoggingEvent> loggingEventQueue = new LinkedBlockingQueue<LoggingEvent>();
    private static HTTPAppender instance;
    private static Thread thread = null;

    private HTTPSink client;
    
    private static JsonLayout layout = new JsonLayout();
    
    private String url;
    private String array;
    private String auth;
    private String authUsername;
    private String authPassowrd;
    
    private static final Pattern pattern = Pattern.compile("(\\w+)\\s*=\\s*(\"[^\"]*\"|'[^']*')");
    private String add;

    static {
        try {
            layout.setHostName(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {}
        
        thread = new Thread(new Runnable() {
            public void run() {
                processQueue();
            }
        });

        thread.setDaemon(true);
        thread.start();
    }

    private static void processQueue() {
        while (true) {
            try {
                LoggingEvent event = loggingEventQueue.poll(1L, TimeUnit.SECONDS);
                if (event != null) {
                    instance.processEvent(event);
                }
            } catch (InterruptedException | ParseException e) {
                LogLog.error(e.getMessage(), e);
            }
        }
    }

    private final void processEvent(LoggingEvent loggingEvent) throws ParseException {
        if (loggingEvent != null) {
            JsonPOSTRequest request = client.toJsonPOSTRequest(layout.format(loggingEvent));

            Exception exception = client.sendBatch(Arrays.asList(request));
            
            if(exception != null)
                LogLog.error(exception.getMessage(), exception);
        }
    }

    public synchronized void close() {
        if (this.closed) {
            return;
        }

        closeWS();
        thread.interrupt();

        LogLog.debug("Closing appender [" + name + "].");
        this.closed = true;
    }

    private void closeWS() {
        try {
            
        } catch (Exception ex) {
            LogLog.error("Error while closing WebServiceAppender [" + name + "].", ex);
        }
    }

    public boolean requiresLayout() {
        return false;
    }

    @Override
    public void activateOptions() {
        instance = this;
        try {
            LogLog.debug("Getting web service properties.");
            
            Properties properties = new Properties();
            if(url != null)
                properties.setProperty("url", url);
            if(array != null)
                properties.setProperty("as-array", array);
            if(auth != null)
                properties.setProperty("auth", auth);
            if(authUsername != null)
                properties.setProperty("auth.user", authUsername);
            if(authPassowrd != null)
                properties.setProperty("auth.password", authPassowrd);
            
            if(add != null) {
                Matcher matcher = pattern.matcher(add);
                
                while (matcher.find()) {
                    String value = matcher.group(2);
                    if((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'")))
                        value = value.substring(1, value.length() - 1);
                    
                    properties.setProperty("add." + matcher.group(1), value );
                }
            }

            client = new HTTPSink();
            client.config(properties);
            client.setLogging(false);
            
            LogLog.warn(properties.toString());
        } catch (Exception ex) {
            LogLog.error("Error while activating options for HTTPAppender [" + name + "].", ex);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getArray() {
        return array;
    }

    public void setArray(String array) {
        this.array = array;
    }

    public String isAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getAuthUsername() {
        return authUsername;
    }

    public void setAuthUsername(String authUsername) {
        this.authUsername = authUsername;
    }

    public String getAuthPassowrd() {
        return authPassowrd;
    }

    public void setAuthPassowrd(String authPassowrd) {
        this.authPassowrd = authPassowrd;
    }

    public String getAdd() {
        return add;
    }

    public void setAdd(String add) {
        this.add = add;
    }

    @Override
    protected void append(LoggingEvent event) {
        loggingEventQueue.add(event);
    }

    @Override
    public void finalize() {
        close();
        super.finalize();
    }
}
