package ch.cern.spark;

import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

public class StatusStream<K, V, S, R> extends Stream<R> {
	
	private PairStream<K, S> statuses;
	
	private StatusStream(JavaMapWithStateDStream<K, V, S, R> stateStream) {
		super(stateStream);
		
		this.statuses = PairStream.from(stateStream.stateSnapshots());
	}

	public static<K, V, S, R> StatusStream<K, V, S, R> from(JavaMapWithStateDStream<K, V, S, R> input) {
		return new StatusStream<>(input);
	}

	public PairStream<K, S> getStatuses() {
		return statuses;
	}

}
