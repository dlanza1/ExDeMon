package ch.cern.spark.status.storage.manager;

import java.util.Iterator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;
import org.bouncycastle.util.Arrays;

import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import scala.Tuple2;

public class ZookeeperStatusesOpertaionsF<K extends StatusKey, V, S extends StatusValue> implements VoidFunction<Iterator<Tuple2<Tuple2<K, S>, StatusOperation<K, V>>>> {

	private static final long serialVersionUID = 4641310780508591435L;
	
	private transient final static Logger LOG = Logger.getLogger(ZookeeperStatusesOpertaionsF.class.getName());
	
	private static CuratorFramework client = null;
	
	private static JSONStatusSerializer derializer = new JSONStatusSerializer();
	
	public ZookeeperStatusesOpertaionsF(Properties props) {
        String zkConnString = props.getProperty("connection_string");
        int timeout_ms = (int) props.getLong("timeout_ms", 20000);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkConnString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(timeout_ms)
                .build();
		client.start();
		LOG.info("Client started. Connection string: " + zkConnString);
		
		derializer = new JSONStatusSerializer();
	}

	@Override
	public void call(Iterator<Tuple2<Tuple2<K, S>, StatusOperation<K, V>>> tuples) throws Exception {
		while (tuples.hasNext()) {
			Tuple2<Tuple2<K, S>, StatusOperation<K, V>> tuple = tuples.next();
			
			@SuppressWarnings("unchecked")
			Tuple2<StatusKey, StatusValue> keyValue = (Tuple2<StatusKey, StatusValue>) tuple._1;
			StatusOperation<K, V> op = tuple._2;
			
			if(op.getOp().equals(Op.LIST)) {
				if(op.filter(keyValue)) {
					writeResult(op.getId(), keyValue._1);
				}
			}
		}
	}

	private void writeResult(String id, StatusKey key) throws Exception {
		String path = "/id="+id+"/results";
		
		String keyAsString = new String(derializer.fromKey(key)).concat("\n");
		
		if(client.checkExists().forPath(path) != null) {
			byte[] currentData = client.getData().forPath(path);
			
			if(currentData.length > 10000) {
				client.setData().forPath("/id="+id+"/status", "ERROR results maximun size reached".getBytes());
				return;
			}
			
			client.setData().forPath(path, Arrays.concatenate(currentData, keyAsString.getBytes()));
		}else{
			client.create().forPath(path, keyAsString.getBytes());
		}
	}

}
