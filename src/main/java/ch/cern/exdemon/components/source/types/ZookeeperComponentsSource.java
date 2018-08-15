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
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.components.source.ComponentsSource;
import ch.cern.properties.Properties;
import ch.cern.utils.StringUtils;

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
    protected ConfigurationResult configure(Properties properties) {
        zkConnString = properties.getProperty("connection_string");
        initialization_timeout_ms = properties.getLong("initialization_timeout_ms", 5000);
        timeout_ms = (int) properties.getLong("timeout_ms", 20000);
        confNodeName = properties.getProperty("conf_node_name", CONF_NODE_NAME_DEFAULT);
        
        return properties.warningsIfNotAllPropertiesUsed();
    }

    private void insertComponent(String path, String value) {
        String type = extractProperty(typePattern, path);
        String id = extractProperty(idPattern, path);
        
        if(value != null && type == null) {
            LOG.debug("Component not added because is missing type in the path: " + path);
            return;
        }
        
        if(value != null && id == null) {
            LOG.debug("Component not added because is missing id in the path: " + path);
            return;
        }
        
        Properties componentProps = Properties.fromJson(value);
        
        if(value != null && componentProps == null) {
            LOG.error("Not a valid JSON at path " + path + ". Value: " + value);
            remove(Type.valueOf(type.toUpperCase()), id);
            return;
        }
            
        register(Type.valueOf(type.toUpperCase()), id, componentProps);
        
        cleanStats(path);
    }

    private void removeComponent(String path) {
        String type = extractProperty(typePattern, path);
        String id = extractProperty(idPattern, path);
        
        if(type == null) {
            LOG.error("Component not removed because is missing type in the path: " + path);
            return;
        }
        
        if(id == null) {
            LOG.error("Component not removed because is missing id in the path: " + path);
            return;
        }
        
        remove(Type.valueOf(type.toUpperCase()), id);
        
        clean(path);
    }
    
    private void clean(String path) {
        String rootPath = path.replace("/" + confNodeName, "");
        
        try {
            if(client.checkExists().forPath(rootPath) != null)
                client.delete().deletingChildrenIfNeeded().forPath(rootPath);
        } catch (Exception e) {
            LOG.error("Error when cleaning component: " + rootPath, e);
        }
    }
    
    private void cleanStats(String path) {
        String rootPath = path.replace("/" + confNodeName, "/");

        try {
            if(client.checkExists().forPath(rootPath + STATS_NODE_NAME) != null)
                client.delete().forPath(rootPath + STATS_NODE_NAME);
        } catch (Exception e) {
            LOG.error("Error when removing: " + rootPath + STATS_NODE_NAME, e);
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
                if(!isConfigurationNode(path))
                    return;
                
                String value = new String(event.getData().getData());
                insertComponent(path, value);
                
                LOG.debug("Node with data added to the tree: " + path + "=" + value);
            }
            break;
        case NODE_REMOVED:
            String removedPath = event.getData().getPath();
            if(!isConfigurationNode(removedPath))
                return;
             
            removeComponent(removedPath);
            
            LOG.debug("Node removed from the tree: " + removedPath);
            break;
        case NODE_UPDATED:
            if(event.getData().getData() != null) {
                String path = event.getData().getPath();
                if(!isConfigurationNode(path))
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

    private boolean isConfigurationNode(String path) {
        return path.endsWith("/" + confNodeName);
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
        
        String path = "/type=" + componentRegistrationResult.getComponentType().toString().toLowerCase() 
                    + "/id=" + componentRegistrationResult.getComponentId()
                    + "/" + CONF_RESULT_NODE_NAME;
        String prettryJson = componentRegistrationResult.toJsonString();
    
        try {
            if(client.checkExists().forPath(path) == null) {
                //Might creared by another worker during this time, so if it fails, data is set
                try {
                    client.create().forPath(path, prettryJson.getBytes());
                }catch(Exception e) {
                    client.setData().forPath(path, prettryJson.getBytes());
                }
            }else {
                client.setData().forPath(path, prettryJson.getBytes());
            }
        } catch (Exception e) {
            LOG.error("Error when updating configuration result at: " + path, e);
        }
    }
    
    @Override
    public void addToReport(Type componentType, String componentId, String reportName, String content) {
        super.addToReport(componentType, componentId, reportName, content);
        
        String path = "/type=" + componentType.toString().toLowerCase() 
                    + "/id=" + componentId
                    + "/" + reportName + ".report";
        
        try {
            if(client.checkExists().forPath(path) == null) {
                client.create().forPath(path, content.getBytes());
            }else {
                byte[] currentContentAsBytes = client.getData().forPath(path);
                String currentContent = currentContentAsBytes == null ? "" : new String(currentContentAsBytes);
                
                content = content.concat("\n").concat(currentContent);
                
                content = StringUtils.headLines(content, 50);
                
                client.setData().forPath(path, content.getBytes());
            }
        } catch (Exception e) {
            LOG.error("Error when inserting report: " + path, e);
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
