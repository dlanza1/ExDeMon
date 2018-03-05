package ch.cern.properties.source.types;

import java.io.IOException;
import java.util.LinkedList;
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

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;

@RegisterComponent("zookeeper")
public class ZookeeperPropertiesSource extends PropertiesSource {

    private static final long serialVersionUID = -484558653609471794L;
    
    private transient final static Logger LOG = Logger.getLogger(ZookeeperPropertiesSource.class.getName());
    
    private String zkConnString;
    
    private CuratorFramework client = null;

    private TreeCache cache;

    private long initialization_timeout_ms;

    private Properties currentProperties;

    private int timeout_ms;
    
    private static final Pattern typePattern = Pattern.compile("/type=([^/]+)/");
    private static final Pattern idPattern = Pattern.compile("/id=([^/]+)/");
    private static final Pattern envPattern = Pattern.compile("/env=([^/]+)/");
    private static final Pattern ownerPattern = Pattern.compile("/owner=([^/]+)/");
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
        zkConnString = properties.getProperty("connection_string");
        initialization_timeout_ms = properties.getLong("initialization_timeout_ms", 5000);
        timeout_ms = (int) properties.getLong("timeout_ms", 20000);
        
        properties.confirmAllPropertiesUsed();
    }

    @Override
    public Properties load() throws Exception {
        initialize();
        
        return (Properties) currentProperties.clone();
    }

    @SuppressWarnings("unused")
    private void parseTree(TreeCache cache, String path, Properties properties) {
        ChildData data = cache.getCurrentData(path.equals("") ? "/" : path);
        if(data.getData() != null)
            insertValue(data.getPath(), new String(data.getData()));
        
        Map<String, ChildData> childs = cache.getCurrentChildren(path.equals("") ? "/" : path);
        
        for (Map.Entry<String, ChildData> child : childs.entrySet())
            parseTree(cache, path + "/" + child.getKey(), properties);
    }

    private void insertValue(String path, String value) {
        String type = extractProperty(typePattern, path);
        String id = extractProperty(idPattern, path);
        
        String env = extractProperty(envPattern, path);
        String owner = extractProperty(ownerPattern, path);
        
        if(type == null) {
            LOG.warn("Path not added because is missing type in the path: " + path);
            return;
        }
        
        if(id == null) {
            LOG.warn("Path not added because is missing id in the path: " + path);
            return;
        }
        
        String key = toPropertyKey(owner, env, id, path);
        
        switch(type) {
        case "schema":
            currentProperties.setProperty("metrics.schema." + key, value);
            break;
        case "metric":
            currentProperties.setProperty("metrics.define." + key, value);
            break;
        case "monitor":
            currentProperties.setProperty("monitor." + key, value);
            break;
        case "actuator":
            currentProperties.setProperty("actuators." + key, value);
            break;
        default:
            return;
        }
    }
    
    private void removeValue(String path) {
        String type = extractProperty(typePattern, path);
        String env = extractProperty(envPattern, path);
        String id = extractProperty(idPattern, path);
        String owner = extractProperty(ownerPattern, path);
        
        if(type == null) {
            LOG.warn("Path not removed because is missing type in the path: " + path);
            return;
        }
        
        if(id == null) {
            LOG.warn("Path not removed because is missing id in the path: " + path);
            return;
        }
        
        String key = toPropertyKey(owner, env, id, path);
        
        switch(type) {
        case "schema":
            currentProperties.remove("metrics.schema." + key);
            break;
        case "metric":
            currentProperties.remove("metrics.define." + key);
            break;
        case "monitor":
            currentProperties.remove("monitor." + key);
            break;
        case "actuator":
            currentProperties.remove("actuators." + key);
            break;
        default:
            return;
        }
    }

    public static String toPropertyKey(String owner, String env, String id, String path) {
        String full_id = id;
        if(owner != null || env != null) {
            if(env != null)
                full_id = env + "_" + full_id;
            else
                full_id = "UNKNOWN_" + full_id;
            if(owner != null)
                full_id = owner + "_" + full_id;
            else
                full_id = "UNKNOWN_" + full_id;
        }
        
        LinkedList<String> elementsForKey = new LinkedList<>();
        elementsForKey.add(full_id);
        
        String[] nodes = path.split("/");
        for (String node : nodes)
            if(node.length() > 0 && !Pattern.matches("[a-z]+=.*", node))
                elementsForKey.add(node);
        
        return String.join(".", elementsForKey);
    }

    private String extractProperty(Pattern pattern, String string) {
        Matcher typeMatcher = pattern.matcher(string);
        
        String value = null;
        
        if(typeMatcher.find())
            value = typeMatcher.group(1);
        
        return value;
    }

    private void initialize() throws Exception {
        if(client != null)
            return;
        
        client = CuratorFrameworkFactory.builder()
                                .connectString(zkConnString)
                                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                                .sessionTimeoutMs(timeout_ms)
                                .build();
        client.start();
        LOG.info("Client started. Connection string: " + zkConnString);
        
        initialiceCache();
    }

    private void initialiceCache() throws Exception {
        Semaphore sem = new Semaphore(0);
        
        cache = new TreeCache(client, "/");
        cache.start();
        
        currentProperties = new Properties();
        
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                switch (event.getType()) {
                case INITIALIZED:
                    LOG.info("TreeCache initialized");
                    sem.release();
                    break;
                case CONNECTION_LOST:
                    LOG.error("Conection lost");
                    close();
                    break;
                case CONNECTION_RECONNECTED:
                    LOG.warn("Conection reconnected");
                    break;
                case CONNECTION_SUSPENDED:
                    LOG.error("Conection suspended");
                    close();
                    break;
                case NODE_ADDED:
                    if(event.getData().getData() != null) {
                        String path = event.getData().getPath();
                        String value = new String(event.getData().getData());
                        insertValue(path, value);
                    
                        LOG.trace("Node with data added to the tree: " + path + "=" + value);
                    }
                    break;
                case NODE_REMOVED:
                    removeValue(event.getData().getPath());
                    LOG.trace("Node removed from the tree: " + event.getData().getPath());
                    break;
                case NODE_UPDATED:
                    if(event.getData().getData() != null) {
                        String path = event.getData().getPath();
                        String value = new String(event.getData().getData());
                        insertValue(path, value);
                    
                        LOG.trace("Node with data updated in the tree: " + path + "=" + value);
                    }
                    break;
                default:
                    break;
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
    
    private void close() {
        currentProperties = null;
        cache.close();
        cache = null;
        client.close();
        client = null;
    }
    
}
