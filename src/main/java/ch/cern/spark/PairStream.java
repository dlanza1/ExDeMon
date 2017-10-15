package ch.cern.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

public class PairStream<K, V> {
	
	private JavaPairDStream<K, V> pairStream;
	
	private PairStream(JavaPairDStream<K, V> stream) {
		this.pairStream = stream;
	}

	public static<K, V> PairStream<K, V> from(JavaPairDStream<K, V> input) {
		return new PairStream<>(input);
	}

	public<S, R> StatusStream<K, V, S, R> mapWithState(
			JavaRDD<Tuple2<K, S>> initialStates,
			Function4<Time, K, Optional<V>, State<S>, Optional<R>> updateStatusFunction, 
			java.time.Duration dataExpirationPeriod) {

        StateSpec<K, V, S, R> statusSpec = StateSpec
                .function(updateStatusFunction)
                .initialState(initialStates.rdd())
                .timeout(new Duration(dataExpirationPeriod.toMillis()));
        
		return StatusStream.from(pairStream.mapWithState(statusSpec));
	}

	public void foreachRDD(VoidFunction<JavaPairRDD<K, V>> function) {
		pairStream.foreachRDD(function);
	}

	public JavaPairDStream<K, V> asJavaPairDStream() {
		return pairStream;
	}

	public void save(String checkpointDir) {
		foreachRDD(rdd -> RDDHelper.save(rdd, checkpointDir));
	}

	public<R> Stream<R> transform(Function2<JavaPairRDD<K, V>, Time, JavaRDD<R>> transformFunc) {
		return Stream.from(pairStream.transform(transformFunc));
	}

}
