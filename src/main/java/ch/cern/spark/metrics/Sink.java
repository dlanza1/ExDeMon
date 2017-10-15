package ch.cern.spark.metrics;

import ch.cern.spark.Stream;

public interface Sink<T> {

	public void sink(Stream<T> outputStream);
	
}
