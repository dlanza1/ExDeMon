package ch.cern.spark.status;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.bouncycastle.util.Arrays;

import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.utils.StringUtils;

public abstract class UpdateStatusFunction<K extends StatusKey, V, S extends StatusValue, R>
    implements Function4<Time, K, Optional<StatusOperation<K, V>>, State<S>, Optional<RemoveAndValue<K, R>>> {

    private static final long serialVersionUID = 8556057397769787107L;
    
    private transient final static Logger LOG = Logger.getLogger(UpdateStatusFunction.class.getName());
    
    private static JSONStatusSerializer derializer = new JSONStatusSerializer();
    private static CuratorFramework client = null;
    private String zkConnString;
    private int timeout_ms;
    
    @Override
    public Optional<RemoveAndValue<K, R>> call(Time time, K key, Optional<StatusOperation<K, V>> actionOrValue, State<S> state) throws Exception {
        if(state.isTimingOut()) {
            Optional<R> result = timingOut(time, key, state.get());
            
            return Optional.of(new RemoveAndValue<>(key, result));
        }
        
        try {
            if(actionOrValue.get().getOp().equals(Op.REMOVE)) {
                state.remove();
                writeResult(actionOrValue.get().getId(), "keys_removed", key, null);
            }else if(actionOrValue.get().getOp().equals(Op.SHOW)) {
                writeResult(actionOrValue.get().getId(), "values", key, state.get());
            }else if(actionOrValue.get().getOp().equals(Op.UPDATE)) {
                StatusImpl<S> status = new StatusImpl<S>(state, time);
                
                Optional<R> result = toOptional(update(key, actionOrValue.get().getValue(), status));
                
                if(status.isRemoved())
                    return Optional.of(new RemoveAndValue<>(key, result));
                else
                    return Optional.of(new RemoveAndValue<>(null, result));
            }
        }catch(Exception e) {
            LOG.error("When appliying operation " + actionOrValue.get(), e);
        }
        
        return Optional.absent();
    }

    protected abstract java.util.Optional<R> update(K key, V value, State<S> status) throws Exception;
    
    protected Optional<R> timingOut(Time time, K key, S state) {
        return Optional.empty();
    }
    
    private Optional<R> toOptional(java.util.Optional<R> result) {
        return result.isPresent() ? Optional.of(result.get()) : Optional.empty();
    }

    public void configStatusesOp(Properties props) {
        zkConnString = props.getProperty("connection_string");
        timeout_ms = (int) props.getLong("timeout_ms", 20000);
    }
    
    private void writeResult(String id, String targetPath, StatusKey key, StatusValue value) throws Exception {
        if(id.equals("old_remove"))
            return;
        
        getClient();
        
        String path = "/id="+id+"/" + targetPath;
        
        String keyAsString = new String(derializer.fromKey(key));
        
        String result;
        if(value != null) {
            String valueAsString = new String(derializer.fromValue(value));
            String keyValueAsString = "{\"key\":"+keyAsString+",\"value\":"+valueAsString+"}\n";
            
            result = keyValueAsString;
        }else {
            result = keyAsString;
        }
        
        if(client.checkExists().forPath(path) != null) {
            byte[] currentData = client.getData().forPath(path);
            
            if(currentData.length > 1000000) {
                client.setData().forPath("/id="+id+"/status", "WARNING results maximun size reached".getBytes());
                return;
            }
            
            client.setData().forPath(path, Arrays.concatenate(currentData, result.getBytes()));
            
            finishOperation(id, targetPath);
        }else{
            client.create().forPath(path, result.getBytes());
            
            finishOperation(id, targetPath);
        }
    }
    
    private void finishOperation(String id, String targetPath) throws Exception {
        byte[] keysData = client.getData().forPath("/id="+id+"/keys");
        byte[] targetData = client.getData().forPath("/id="+id+"/" + targetPath);
        
        int numKeys = StringUtils.countLines(new String(keysData));
        int numTargets = StringUtils.countLines(new String(targetData));
        
        if(numKeys == numTargets)
            client.setData().forPath("/id="+id+"/status", "DONE".getBytes());
    }

    public CuratorFramework getClient() {
        if(client == null)
            initClient();
        
        return client;
    }
    
    private void initClient() {
        client = CuratorFrameworkFactory.builder()
                        .connectString(zkConnString)
                        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .sessionTimeoutMs(timeout_ms)
                        .build();
        client.start();
        
        LOG.info("Client started. Connection string: " + zkConnString);
    }

}
