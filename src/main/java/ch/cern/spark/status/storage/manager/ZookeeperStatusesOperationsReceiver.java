package ch.cern.spark.status.storage.manager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import scala.Tuple2;

public class ZookeeperStatusesOperationsReceiver extends Receiver<StatusOperation<StatusKey, ?>> {

    private static final long serialVersionUID = -6756122444455084725L;
    
    private transient final static Logger LOG = Logger.getLogger(ZookeeperStatusesOperationsReceiver.class.getName());
    
    public static String PARAM = "spark.statuses.operations.zookeeper";
    
    private String zkConnString;
    private long initialization_timeout_ms;
    private int timeout_ms;
    
    private static CuratorFramework client = null;

    private static TreeCache cache;

    private static JsonParser parser = new JsonParser();
	private static JSONStatusSerializer derializer = new JSONStatusSerializer();

    public ZookeeperStatusesOperationsReceiver(Properties properties) {
        super(StorageLevel.MEMORY_ONLY());

        zkConnString = properties.getProperty("connection_string");
        initialization_timeout_ms = properties.getLong("initialization_timeout_ms", 5000);
        timeout_ms = (int) properties.getLong("timeout_ms", 20000);
    }
    
    @Override
    public void onStart() {
        try {
            initialize();
        }catch(Exception e) {
            LOG.error("Error initializing", e);
            close();
            
            throw new RuntimeException();
        }
    }
    
    private void initialize() throws Exception {
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
    
    private void initialiceCache() throws Exception {
        Semaphore sem = new Semaphore(0);
        
        cache = new TreeCache(client, "/");
        cache.start();
        
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                try {
                    tryApply(event);
                }catch(Exception e) {
                    LOG.error("Error when applying tree event", e);
                    close();
                }
            }

            private void tryApply(TreeCacheEvent event) {
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
                case NODE_UPDATED:
                    String path = event.getData().getPath();
                    byte[] data = event.getData().getData();
                    
                    if(path.endsWith("/op")) {
                    	String rootPath = path.substring(0, path.length() - "op".length());
                        
                        try {
							addOperation(rootPath, new String(data));
							
							LOG.info("New operation at " + path + " => " + new String(data));
						} catch (Exception e) {
							LOG.error(rootPath, e);
							
							try {
								setNodeData(rootPath + "status", ("ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage()).getBytes());
							} catch (Throwable e1) {
								LOG.error(rootPath + " when setting error message", e1);
							}
						}
                    }
                    break;
                case NODE_REMOVED:
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

    protected void addOperation(String rootPath, String opString) throws Exception {
    	String id = getId(rootPath);
    	
    	switch(Op.valueOf(opString.toUpperCase())) {
		case REMOVE:
			getKeys(rootPath).stream().forEach(key -> storeOperation(new StatusOperation<>(id, key, Op.REMOVE)));
			setNodeData(rootPath + "status", "DONE".getBytes());
			break;
		case LIST:
			storeOperation(new StatusOperation<>(id, getFilters(rootPath)));
			setNodeData(rootPath + "status", "RECEIVED".getBytes());
			break;
		case UPDATE:
		default:
			throw new Exception("Operation " + opString + " not available.");
    	}
    }

    private void storeOperation(StatusOperation<StatusKey, StatusValue> op) {
    	LOG.info("New operation: " + op);
		
    	store(op);
	}

	private void setNodeData(String path, byte[] bytes) throws Exception {
		if(client.checkExists().forPath(path) != null)
			client.setData().forPath(path, bytes);
		else
			client.create().forPath(path, bytes);
	}

	private List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> getFilters(String rootPath) throws Exception {
    	List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
    	
    	byte[] filtersAsBytes = client.getData().forPath(rootPath + "filters");
        if(filtersAsBytes == null)
            throw new Exception("filters are empty");
        
        String filtersAsString = new String(filtersAsBytes);
        
        for(String filterAsString : filtersAsString.split("\n")) {
        	if(filterAsString.trim().length() <= 0)
        		continue;
        	
        	String[] fieldFields = filterAsString.trim().split(" ");
        	
        	if(fieldFields.length < 2)
        		throw new Exception("filter of type " + fieldFields[0] + " does not exist");
        	
        	switch (fieldFields[0]) {
			case "class":
				filters.add(new ClassNameStatusKeyFilter(fieldFields[1]));
				break;
			case "id":
				filters.add(new IDStatusKeyFilter(fieldFields[1]));
				break;
			case "pattern":
				filters.add(new ToStringPatternStatusKeyFilter(fieldFields[1]));
				break;
			default:
				throw new Exception("filter of type " + fieldFields[0] + " does not exist");
			}
        }
    	
		return filters;
	}

	private String getId(String rootPath) {
		return rootPath.split("/")[1].replace("id=", "");
	}

	private List<StatusKey> getKeys(String rootPath) throws Exception {
    	LinkedList<StatusKey> keys = new LinkedList<>();
    	
        byte[] jsonKeysAsString = client.getData().forPath(rootPath + "keys");
        if(jsonKeysAsString == null)
            throw new Exception("keys are empty");
        
        JsonElement element = parser.parse(new String(jsonKeysAsString));
        
        if(element.isJsonObject()) {
        	keys.add(derializer.toKey(jsonKeysAsString));
        }else if(element.isJsonArray()) {
        	JsonArray jsonArray = element.getAsJsonArray();
        	
        	for (JsonElement jsonElement : jsonArray) {
        		if(!jsonElement.isJsonObject())
        			continue;
        		
        		keys.add(derializer.toKey(jsonElement.toString().getBytes()));
			}
        }
        
		return keys;
	}

	@Override
    public void onStop() {
        close();
    }

    private void close() {
        if(cache != null)
            cache.close();
        cache = null;
        if(client != null)
            client.close();
        client = null;
    }
    
}
