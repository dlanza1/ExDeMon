package ch.cern.spark;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.dstream.InputDStream;

import scala.Option;
import scala.reflect.ClassTag;

public class TestInputStream<T> extends InputDStream<T> {

    private Batches<T> output;
    
    private long batchDuration;

    public TestInputStream(StreamingContext _ssc, ClassTag<T> evidence$1, Batches<T> input, long batchDuration) {
        super(_ssc, evidence$1);
        
        this.output = input;
        this.batchDuration = batchDuration;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Option<RDD<T>> compute(Time time) {
        int inputIndex = (int) (time.milliseconds() / batchDuration) - 1;
        
        if(inputIndex < output.size()){
            List<T> selectedOutput = output.get(inputIndex);
            
            return Option.apply(getSparkContext().parallelize(selectedOutput).rdd());
        }else{
            return Option.empty();
        }
    }

    public JavaSparkContext getSparkContext() {
        return JavaSparkContext.fromSparkContext(context().sparkContext());
    }
    
    public static class JTestInputStream<T> extends JavaInputDStream<T>{
        
        private static final long serialVersionUID = 6668075506727419418L;

        public JTestInputStream(StreamingContext _ssc, ClassTag<T> classTag, Batches<T> inputBatches, long batchDuration) {
            super(new TestInputStream<>(_ssc, classTag, inputBatches, batchDuration), classTag);
        }

    }
    
}
