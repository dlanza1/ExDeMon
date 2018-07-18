package ch.cern.exdemon.components.source.types;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import ch.cern.exdemon.components.ComponentRegistrationResult;
import ch.cern.exdemon.components.ComponentRegistrationResult.Status;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.components.source.ComponentsSource;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

@RegisterComponentType("zookeeper")
public class ZookeeperComponentsSource extends ComponentsSource {

    private static final long serialVersionUID = 8957322536482029694L;
    
    private transient final static Logger LOG = Logger.getLogger(ZookeeperComponentsSource.class.getName());

    private String zkConnString;
    
    private CuratorFramework client = null;

    private TreeCache cache;

    private long initialization_timeout_ms;

    private int timeout_ms;
    
    private static final Pattern typePattern = Pattern.compile("/type=([^/]+)/");
    private static final Pattern idPattern = Pattern.compile("/id=([^/]+)/");

    private static final String CONF_NODE_NAME_DEFAULT = "config";
    private String confNodeName;
    
    private static final String CONF_RESULT_NODE_NAME = "config.result";
    private static final String STATS_NODE_NAME = "stats";
    
    public ZookeeperComponentsSource() {
        super();
    }
    
    @Override
    protected void configure(Properties properties) throws ConfigurationException {
        zkConnString = properties.getProperty("connection_string");
        initialization_timeout_ms = properties.getLong("initialization_timeout_ms", 5000);
        timeout_ms = (int) properties.getLong("timeout_ms", 20000);
        confNodeName = "/" + properties.getProperty("conf_node_name", CONF_NODE_NAME_DEFAULT);
        
        properties.confirmAllPropertiesUsed();
    }

    private void insertComponent(String path, String value) {
        String type = extractProperty(typePattern, path);
        String id = extractProperty(idPattern, path);
        
        if(value != null && type == null) {
            LOG.debug("Path not added because is missing type in the path: " + path);
            return;
        }
        
        if(value != null && id == null) {
            LOG.debug("Path not added because is missing id in the path: " + path);
            return;
        }
        
        Properties componentProps = Properties.fromJson(value);
        
        if(value != null && componentProps == null)
            LOG.warn("Not a valid JSON at path " + path + ". Value: " + value);
        
        register(Type.valueOf(type.toUpperCase()), id, componentProps);
        
        clean(path);
    }

    private void removeComponent(String path) {
        String type = extractProperty(typePattern, path);
        String id = extractProperty(idPattern, path);
        
        if(type == null) {
            LOG.debug("Path not removed because is missing type in the path: " + path);
            return;
        }
        
        if(id == null) {
            LOG.debug("Path not removed because is missing id in the path: " + path);
            return;
        }
        
        remove(Type.valueOf(type.toUpperCase()), id);
        
        clean(path);
    }
    
    private void clean(String path) {
        String rootPath = path.replace("/" + confNodeName, "/");
        
        try {
            if(client.checkExists().forPath(rootPath + CONF_RESULT_NODE_NAME) != null)
                client.delete().forPath(rootPath + CONF_RESULT_NODE_NAME);
            
            if(client.checkExists().forPath(rootPath + STATS_NODE_NAME) != null)
                client.delete().forPath(rootPath + STATS_NODE_NAME);
        } catch (Exception e) {
            LOG.error("Error when cleaning component node", e);
        }
    }

    private String extractProperty(Pattern pattern, String string) {
        Matcher matcher = pattern.matcher(string);
        
        String value = null;
        
        if(matcher.find())
            value = matcher.group(1);
        
        return value;
    }

    @Override
    public synchronized void initialize() throws Exception {
        if(client == null){
            client = CuratorFrameworkFactory.builder()
                                                .connectString(zkConnString)
                                                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                                                .sessionTimeoutMs(timeout_ms)
                                                .build();
            
            client.start();
            LOG.info("Client started. Connection string: " + zkConnString);
            
            initialiceCache();
        }
    }
    
    @VisibleForTesting
    protected final void applyEvent(Semaphore sem, TreeCacheEvent event) {
        switch (event.getType()) {
        case INITIALIZED:
            LOG.info("TreeCache initialized");
            sem.release();
            break;
        case CONNECTION_LOST:
            LOG.error("Conection lost");
            break;
        case CONNECTION_RECONNECTED:
            LOG.warn("Conection reconnected");
            break;
        case CONNECTION_SUSPENDED:
            LOG.error("Conection suspended");
            break;
        case NODE_ADDED:
            byte[] data = event.getData().getData();
            
            if(data != null && data.length > 0) {
                String path = event.getData().getPath();
                if(!path.endsWith(confNodeName))
                    return;
                
                String value = new String(event.getData().getData());
                insertComponent(path, value);
                
                LOG.debug("Node with data added to the tree: " + path + "=" + value);
            }
            break;
        case NODE_REMOVED:
            removeComponent(event.getData().getPath());
            LOG.info("Node removed from the tree: " + event.getData().getPath());
            break;
        case NODE_UPDATED:
            if(event.getData().getData() != null) {
                String path = event.getData().getPath();
                if(!path.endsWith(confNodeName))
                    return;
                
                String value = new String(event.getData().getData());
                insertComponent(path, value);
                
                LOG.debug("Node with data updated in the tree: " + path + "=" + value);
            }
            break;
        default:
            break;
        }
    }

    private void initialiceCache() throws Exception {
        Semaphore sem = new Semaphore(0);
        
        cache = new TreeCache(client, "/");
        cache.start();
        
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                try {
                    applyEvent(sem, event);
                }catch(Exception e) {
                    LOG.error("Error when applying tree event", e);
                    close();
                }
            }
        };
        cache.getListenable().addListener(listener);
        
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                sem.release();
            }
        };
        new Timer(true).schedule(task, initialization_timeout_ms);
        
        sem.acquire();
        
        Map<String, ChildData> currentChildren = cache.getCurrentChildren("/");
        if(currentChildren == null && task.scheduledExecutionTime() > System.currentTimeMillis())
            throw new IOException("Initialization error, parent path may not exist");
        
        if(currentChildren == null)
            throw new IOException("Initialization timed-out, connection string is wrong or nodes/port are not reachable?");
    }
    
    @Override
    protected void pushComponentRegistrationResult(ComponentRegistrationResult componentRegistrationResult) {
        super.pushComponentRegistrationResult(componentRegistrationResult);
        
        if(componentRegistrationResult.getStatus().equals(Status.EXISTING))
            return;
        
        try {
            String path = "/type=" + componentRegistrationResult.getComponentType().toString().toLowerCase() 
                        + "/id=" + componentRegistrationResult.getComponentId()
                        + "/" + CONF_RESULT_NODE_NAME;
            String prettryJson = componentRegistrationResult.toJsonString();
        
            if(client.checkExists().forPath(path) == null)
                client.create().forPath(path, prettryJson.getBytes());
            else
                client.setData().forPath(path, prettryJson.getBytes());
        } catch (Exception e) {
            LOG.error("Error when updating configuration result", e);
        }
    }
    
    @Override
    public void close() {
        if(cache != null)
            cache.close();
        cache = null;
        if(client != null)
            client.close();
        client = null;
    }
    
}
