package ch.cern.spark.status.storage.manager;

import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bouncycastle.util.Arrays;

import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import scala.Tuple2;

public class ZookeeperStatusesOpertaionsF<K extends StatusKey, V, S extends StatusValue> implements VoidFunction<JavaPairRDD<Tuple2<K,S>,StatusOperation<K,V>>> {

	private static final long serialVersionUID = 4641310780508591435L;
	
	private transient final static Logger LOG = Logger.getLogger(ZookeeperStatusesOpertaionsF.class.getName());
	
	private static CuratorFramework client = null;
	
	private static JSONStatusSerializer derializer = new JSONStatusSerializer();

	private String zkConnString;

	private int timeout_ms;
	
	public ZookeeperStatusesOpertaionsF(Properties props) {
        zkConnString = props.getProperty("connection_string");
        timeout_ms = (int) props.getLong("timeout_ms", 20000);
        initClient();
		
		derializer = new JSONStatusSerializer();
	}
	
	@Override
	public void call(JavaPairRDD<Tuple2<K, S>, StatusOperation<K, V>> rdd) throws Exception {
		JavaFutureAction<Void> foreachFuture = rdd.foreachPartitionAsync(rddPart -> call(rddPart));
		JavaFutureAction<List<String>> distinctFuture = rdd.map(o -> o._2.getId()).distinct().collectAsync();
		
		new Thread() {
			public void run() {
				try {
					foreachFuture.get();

					for (String id : distinctFuture.get())
						finishOperation(id);
				} catch (Exception e) {
					LOG.error(e);
				}
			}
		}.start();
	}

	private void finishOperation(String id) throws Exception {
		String currentStatus = new String(client.getData().forPath("/id=" + id + "/ops"));
		if(currentStatus.startsWith("ERROR"))
			return;
		
		client.setData().forPath("/id=" + id + "/status", "DONE".getBytes());
		
		String[] ops = new String(client.getData().forPath("/id=" + id + "/ops")).trim().split(" ");
		
		if(ops.length > 1) {
			String leftOps = null;
			for (int i = 1; i < ops.length; i++)
				leftOps = leftOps == null ? ops[i] : leftOps + " " + ops[i];
			
			client.setData().forPath("/id=" + id + "/ops", leftOps.getBytes());
		}
	}
	
	public void call(Iterator<Tuple2<Tuple2<K, S>, StatusOperation<K, V>>> tuples) {
		while (tuples.hasNext()) {
		    try {
    			Tuple2<Tuple2<K, S>, StatusOperation<K, V>> tuple = tuples.next();
    			
    			@SuppressWarnings("unchecked")
    			Tuple2<StatusKey, StatusValue> keyValue = (Tuple2<StatusKey, StatusValue>) tuple._1;
    			StatusOperation<K, V> op = tuple._2;
    			
    			if(op.getOp().equals(Op.LIST)) {
    				if(op.filter(keyValue)) {
    					writeListResult(op.getId(), keyValue._1);
    				}
    			}
		    } catch (Exception e) {
                LOG.error(e);
            }
		}
	}

    private void writeListResult(String id, StatusKey key) throws Exception {
		getClient();
		
		String path = "/id="+id+"/keys";
		
		String keyAsString = new String(derializer.fromKey(key)).concat("\n");
		
		if(client.checkExists().forPath(path) != null) {
			byte[] currentData = client.getData().forPath(path);
			
			if(currentData.length > 100000) {
				client.setData().forPath("/id="+id+"/status", "WARNING results maximun size reached".getBytes());
				return;
			}
			
			client.setData().forPath(path, Arrays.concatenate(currentData, keyAsString.getBytes()));
		}else{
			client.create().forPath(path, keyAsString.getBytes());
		}
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

	public static <K extends StatusKey, V, S extends StatusValue> void apply(JavaSparkContext context, JavaPairDStream<K, S> statuses, JavaDStream<StatusOperation<K, V>> operations) {
        JavaPairDStream<String, StatusOperation<K, V>> listOperations = operations
				.filter(op -> op.getOp().equals(Op.LIST))
				.mapToPair(op -> new Tuple2<>("filter", op));

		Properties zooStatusesOpFProps = Properties.from(context.getConf().getAll()).getSubset(ZookeeperStatusesOperationsReceiver.PARAM);
		if(zooStatusesOpFProps.size() > 0)
			statuses.mapToPair(s -> new Tuple2<>("filter", s))
				.leftOuterJoin(listOperations)
				.filter(t -> t._2._2.isPresent())
				.mapToPair(t -> new Tuple2<>(t._2._1, t._2._2.get()))
				.foreachRDD(new ZookeeperStatusesOpertaionsF<K, V, S>(zooStatusesOpFProps));
		
	}

}
