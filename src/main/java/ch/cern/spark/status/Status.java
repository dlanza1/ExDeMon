package ch.cern.spark.status;

import java.io.IOException;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Driver;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.storage.StatusesStorage;
import ch.cern.spark.status.storage.manager.ZookeeperStatusesOpertaionsF;
import scala.Option;
import scala.Tuple2;

public class Status {
	
	public static final String STATUSES_EXPIRATION_PERIOD_PARAM = "spark.cern.streaming.status.timeout";
	
	private transient final static Logger LOG = Logger.getLogger(Status.class.getName());

	public static<K extends StatusKey, V, S extends StatusValue, R> StateDStream<K, V, S, R> map(
			Class<K> keyClass,
			Class<S> statusClass,
			JavaDStream<StatusOperation<K, V>> operations,
			UpdateStatusFunction<K, V, S, R> updateStatusFunction) 
					throws ClassNotFoundException, IOException, ConfigurationException {
		
		JavaSparkContext context = JavaSparkContext.fromSparkContext(operations.context().sparkContext());
		
		Optional<StatusesStorage> storageOpt = getStorage(context);
		if(!storageOpt.isPresent())
			throw new ConfigurationException("Storage needs to be configured");
		StatusesStorage storage = storageOpt.get();

        JavaDStream<StatusOperation<K, V>> updatesAndRemoves = operations.filter(op -> op.getOp().equals(Op.UPDATE) || op.getOp().equals(Op.REMOVE));
        JavaPairDStream<K, StatusOperation<K, V>> updatesAndRemovesKeyed = updatesAndRemoves.mapToPair(op -> new Tuple2<>(op.getKey(), op));
        
        //Load initial state from external storage
		JavaPairRDD<K, S> initialStates = storage.load(context, keyClass, statusClass);
        
		//Map values and remove states
        StateSpec<K, StatusOperation<K, V>, S, RemoveAndValue<K, R>> statusSpec = StateSpec.function(updateStatusFunction).initialState(initialStates.rdd());
        statusSpec.numPartitions(10);
        
        Option<Duration> timeout = getStatusExpirationPeriod(context);
        if(timeout.isDefined())
            statusSpec = statusSpec.timeout(timeout.get());
        
        JavaMapWithStateDStream<K, StatusOperation<K, V>, S, RemoveAndValue<K, R>> statusStream = updatesAndRemovesKeyed.mapWithState(statusSpec);
        
        //Keys that has been removed while mapping with states
        JavaDStream<K> keysToRemove = statusStream.filter(rv -> rv.isRemoveAction()).map(rv -> rv.getKey());
        
        //Union them with requested keys to remove 
        JavaDStream<StatusOperation<K, V>> requestedRemoves = updatesAndRemoves.filter(op -> op.getOp().equals(Op.REMOVE));
        keysToRemove = keysToRemove.union(requestedRemoves.map(op -> op.getKey()));
        
        //Remove all from external storage
        keysToRemove.foreachRDD(rdd -> storage.remove(rdd));    
        
        //Save statuses
        statusStream.stateSnapshots().foreachRDD((rdd, time) -> storage.save(rdd, time));
        
        //Statuses operations (list)
        JavaPairDStream<String, StatusOperation<K, V>> listOperations = operations
        																		.filter(op -> op.getOp().equals(Op.LIST))
        																		.mapToPair(op -> new Tuple2<>("filter", op));
        
        Properties zooStatusesOpFProps = Properties.from(context.getConf().getAll()).getSubset(Driver.STATUSES_OPERATIONS_RECEIVER_PARAM);
		statusStream.stateSnapshots().mapToPair(s -> new Tuple2<>("filter", s))
															.leftOuterJoin(listOperations)
															.filter(t -> t._2._2.isPresent())
															.mapToPair(t -> new Tuple2<>(t._2._1, t._2._2.get()))
															.foreachRDD(rdd -> {
																long count = rdd.count();
																
																if(count > 0)
																	LOG.info("Processing list operations (states " + count + ")");
																
																rdd.foreachPartitionAsync(new ZookeeperStatusesOpertaionsF<K, V, S>(zooStatusesOpFProps));
															});
		
		return new StateDStream<>(statusStream);
	}

    private static Option<Duration> getStatusExpirationPeriod(JavaSparkContext context) {
		SparkConf conf = context.getConf();
		
		Option<String> valueString = conf.getOption(STATUSES_EXPIRATION_PERIOD_PARAM);
		
		if(valueString.isDefined())
		    return Option.apply(new Duration(java.time.Duration.parse(valueString.get()).toMillis()));
		else
		    return Option.empty();
	}

	private static java.util.Optional<StatusesStorage> getStorage(JavaSparkContext context) throws ConfigurationException {
		Properties sparkConf = Properties.from(context.getConf().getAll());
		Properties storageConfig = sparkConf.getSubset(StatusesStorage.STATUS_STORAGE_PARAM);
		
		java.util.Optional<StatusesStorage> storage = ComponentManager.buildOptional(Type.STATUS_STORAGE, storageConfig);
		
		return storage;
	}
	
}
