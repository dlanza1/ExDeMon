package ch.cern.spark;

import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

public class StatusStream<K, V, S, R> {
	
	private JavaMapWithStateDStream<K, V, S, R> stateStream;
	
	private StatusStream(JavaMapWithStateDStream<K, V, S, R> stateStream) {
		this.stateStream = stateStream;
	}

	public static<K, V, S, R> StatusStream<K, V, S, R> from(JavaMapWithStateDStream<K, V, S, R> input) {
		return new StatusStream<>(input);
	}

	public Stream<R> stream() {
		return Stream.from(stateStream);
	}

	public PairStream<K, S> getStatuses() {
		return PairStream.from(stateStream.stateSnapshots());
	}

}
