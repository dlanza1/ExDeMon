package ch.cern.spark.status;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.storage.StatusesStorage;
import ch.cern.spark.status.storage.manager.ZookeeperStatusesOperationsReceiver;
import ch.cern.spark.status.storage.manager.ZookeeperStatusesOpertaionsF;
import scala.Option;
import scala.Tuple2;

public class Status {
	
	public static final String STATUSES_EXPIRATION_PERIOD_PARAM = "spark.cern.streaming.status.timeout";

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

        JavaDStream<StatusOperation<K, V>> opsWithKey = operations.filter(op -> op.getOp().equals(Op.UPDATE) 
                                                                             || op.getOp().equals(Op.REMOVE)
                                                                             || op.getOp().equals(Op.SHOW));
        JavaPairDStream<K, StatusOperation<K, V>> opsKeyed = opsWithKey.mapToPair(op -> new Tuple2<>(op.getKey(), op));
        
        Properties zooStatusesOpFProps = Properties.from(context.getConf().getAll()).getSubset(ZookeeperStatusesOperationsReceiver.PARAM);
        updateStatusFunction.configStatusesOp(zooStatusesOpFProps);
        
        //Load initial state from external storage
		JavaPairRDD<K, S> initialStates = storage.load(context, keyClass, statusClass);
        
		//Map values and remove states
        StateSpec<K, StatusOperation<K, V>, S, RemoveAndValue<K, R>> statusSpec = StateSpec.function(updateStatusFunction).initialState(initialStates.rdd());
        statusSpec.numPartitions(10);
        
        Option<Duration> timeout = getStatusExpirationPeriod(context);
        if(timeout.isDefined())
            statusSpec = statusSpec.timeout(timeout.get());
        
        JavaMapWithStateDStream<K, StatusOperation<K, V>, S, RemoveAndValue<K, R>> statusStream = opsKeyed.mapWithState(statusSpec);
        
        //Keys that has been removed while mapping with states
        JavaDStream<K> keysToRemove = statusStream.filter(rv -> rv.isRemoveAction()).map(rv -> rv.getKey());
        
        //Union them with requested keys to remove 
        JavaDStream<StatusOperation<K, V>> requestedRemoves = opsWithKey.filter(op -> op.getOp().equals(Op.REMOVE));
        keysToRemove = keysToRemove.union(requestedRemoves.map(op -> op.getKey()));
        
        //Remove all from external storage
        keysToRemove.foreachRDD(rdd -> storage.remove(rdd));    
        
        //Save statuses
        statusStream.stateSnapshots().foreachRDD((rdd, time) -> storage.save(rdd, time));
        
        //Statuses operations (list)
        ZookeeperStatusesOpertaionsF.apply(context, statusStream.stateSnapshots(), operations);
		
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

	private static Optional<StatusesStorage> getStorage(JavaSparkContext context) throws ConfigurationException {
		Properties sparkConf = Properties.from(context.getConf().getAll());
		Properties storageConfig = sparkConf.getSubset(StatusesStorage.STATUS_STORAGE_PARAM);
		
		if(storageConfig.isTypeDefined()) {
		    ComponentBuildResult<StatusesStorage> storageBuildResult = ComponentTypes.build(Type.STATUS_STORAGE, storageConfig);
		    storageBuildResult.throwExceptionIfPresent();
		    
		    return storageBuildResult.getComponent();
		}else {
		    return Optional.empty();
		}
	}

    public static void configSpark(SparkConf sparkConf, String checkpointDir) {
        if (!sparkConf.contains(StatusesStorage.STATUS_STORAGE_PARAM + ".type")) {
            sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".type", "single-file");
            sparkConf.set(StatusesStorage.STATUS_STORAGE_PARAM + ".path", checkpointDir + "/statuses");
        }
    }
	
}
