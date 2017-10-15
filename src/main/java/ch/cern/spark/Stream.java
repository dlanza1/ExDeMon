package ch.cern.spark;

import java.time.Duration;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.Sink;
import scala.Tuple2;

public class Stream<V> {

	private JavaDStream<V> stream;
	
	private Stream(JavaDStream<V> stream) {
		this.stream = stream;
	}
	
	public static<V> Stream<V> from(JavaDStream<V> input){
		return new Stream<>(input);
	}
	
	public<K> PairStream<K, V> toPair(PairFlatMapFunction<V, K, V> function) {
		return PairStream.from(stream.flatMapToPair(function));
	}

	public<K, S, R> StatusStream<K, V, S, R> mapWithState(
			JavaRDD<Tuple2<K, S>> initialStates,
			PairFlatMapFunction<V, K, V> toPairFunction, 
			Function4<Time, K, Optional<V>, State<S>, Optional<R>> updateStatusFunction,
			Duration dataExpirationPeriod) {

		PairStream<K, V> keyValuePairs = toPair(toPairFunction);
		
		return keyValuePairs.mapWithState(initialStates, updateStatusFunction, dataExpirationPeriod);
	}

	public Stream<V> union(Stream<V> input) {
		return Stream.from(stream.union(input.stream));
	}
	
	public<R> Stream<R> map(Function<V, R> mapFunction) {
		return Stream.from(stream.map(mapFunction));
	}

	public Stream<JSONObject> asJSON() {
		return map(JSONParser::parse);
	}

	public Stream<String> asString() {
		return map(Object::toString);
	}

	public JavaDStream<V> asJavaDStream() {
		return stream;
	}

	public <R> Stream<R> mapS(Function<Stream<V>, Stream<R>> mapStreamFucntion) throws Exception {
		return mapStreamFucntion.call(this);
	}
	
	public void sink(Sink<V> sink) {
		sink.sink(this);
	}
	
}
