package ch.cern.spark.flume;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

public class FlumeEventsParser implements Function<Tuple2<String, byte[]>, FlumeEvent>{
	
	private static final long serialVersionUID = -1642580621226682134L;
	
	FlumeEvent.Decoder decoder = new FlumeEvent.Decoder();
	
	public FlumeEvent call(Tuple2<String, byte[]> tuple2) {
		return decoder.decode(tuple2._2);
	}

	public static JavaDStream<FlumeEvent> apply(JavaPairDStream<String, byte[]> messages) {
		return messages.map(new FlumeEventsParser());
	}
	
}
