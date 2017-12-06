package ch.cern.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDD<T> {
	
	private JavaRDD<T> rdd;

	private RDD(JavaRDD<T> rdd) {
		this.rdd = rdd;
	}

	public static <T> RDD<T> from(JavaRDD<T> rdd) {
		return new RDD<T>(rdd);
	}

	public JavaRDD<T> asJavaRDD(){
		return rdd;
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return JavaSparkContext.fromSparkContext(rdd.context());
	}
	
}
