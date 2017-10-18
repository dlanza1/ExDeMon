package ch.cern.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

public class PairStream<K, V> extends Stream<Tuple2<K, V>>{
	
	public static final String CHECKPPOINT_DURATION_PARAM = "spark.streaming.mapWithState.timeout";
	public static final String CHECKPPOINT_DURATION_DEFAULT = java.time.Duration.ofMinutes(30).toString();
	
	private PairStream(JavaPairDStream<K, V> stream) {
		super(stream.map(tuple -> tuple));
	}

	public static<K, V> PairStream<K, V> from(JavaPairDStream<K, V> input) {
		return new PairStream<>(input);
	}

	public<S, R> StatusStream<K, V, S, R> mapWithState(String id, Function4<Time, K, Optional<V>, State<S>, Optional<R>> updateStatusFunction) throws ClassNotFoundException, IOException {
		
		JavaRDD<Tuple2<K, S>> initialStates = RDD.<Tuple2<K, S>>load(getSparkContext(), id);

        StateSpec<K, V, S, R> statusSpec = StateSpec
							                .function(updateStatusFunction)
							                .initialState(initialStates.rdd())
							                .timeout(getDataExpirationPeriod());
        
        StatusStream<K, V, S, R> statusStream = StatusStream.from(asJavaPairDStream().mapWithState(statusSpec));
        
        	statusStream.getStatuses().save(id);
        		
		return statusStream;
	}

	private Duration getDataExpirationPeriod() {
		JavaSparkContext context = getSparkContext();
		SparkConf conf = context.getConf();
		
		String valueString = conf.get(CHECKPPOINT_DURATION_PARAM, CHECKPPOINT_DURATION_DEFAULT);
		
		return new Duration(java.time.Duration.parse(valueString).toMillis());
	}

	public JavaPairDStream<K, V> asJavaPairDStream() {
		return asJavaDStream().mapToPair(val -> (Tuple2<K, V>) val);
	}

}
