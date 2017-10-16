package ch.cern.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDHelper {
	
	public static final String CHECKPPOINT_DIR_PARAM = "spark.streaming.rdd.checkpoint.directory";

	private static transient FileSystem fs = null;
	
	public static<T> JavaRDD<T> load(JavaSparkContext context, String id)
			throws IOException, ClassNotFoundException {
		
		Path storingPath = getStoringFile(context, id);
		
		return context.parallelize(load(storingPath));
	}

	public static <T> List<T> load(Path storingPath) throws IOException, ClassNotFoundException {
		setFileSystem();

		Path tmpFile = storingPath.suffix(".tmp");

		if (!fs.exists(storingPath) && fs.exists(tmpFile))
			fs.rename(tmpFile, storingPath);

		if (!fs.exists(storingPath))
			return new LinkedList<T>();

		ObjectInputStream is = new ObjectInputStream(fs.open(storingPath));

		@SuppressWarnings("unchecked")
		List<T> elements = (List<T>) is.readObject();

		is.close();

		return elements;
	}

	public static<K, V> void save(JavaPairRDD<K, V> rdd, String id) throws IllegalArgumentException, IOException {
		save(rdd.map(pair -> new Tuple2<K, V>(pair._1, pair._2)), id);
	}
	
	public static<T> void save(JavaRDD<T> input, String id) throws IllegalArgumentException, IOException {
		Path storingPath = getStoringFile(getJavaSparkContext(input), id);
		
		save(storingPath, input.collect());
	}

	public static void save(Path storingPath, List<?> elements) throws IllegalArgumentException, IOException {
		setFileSystem();

		Path tmpFile = storingPath.suffix(".tmp");

		fs.mkdirs(tmpFile.getParent());

		ObjectOutputStream oos = new ObjectOutputStream(fs.create(tmpFile, true));
		oos.writeObject(elements);
		oos.close();

		if (canBeRead(tmpFile)) {
			fs.delete(storingPath, false);
			fs.rename(tmpFile, storingPath);
		}

	}
	
    private static boolean canBeRead(Path tmpFile) throws IOException {
        setFileSystem();
        
        try {
            ObjectInputStream is = new ObjectInputStream(fs.open(tmpFile));
            is.readObject();
            is.close();
            
            return true;
        } catch (Exception e) {
            return false;
        }
    }

	private static void setFileSystem() throws IOException {
		if (fs == null)
			fs = FileSystem.get(new Configuration());
	}

	private static Path getStoringFile(JavaSparkContext context, String id) {
		SparkConf sparkConf = context.getConf();
		
		Path checkpointPath = new Path(sparkConf.get(CHECKPPOINT_DIR_PARAM));
		
		return checkpointPath.suffix("/" + id).suffix("/latest");
	}
	
	private static JavaSparkContext getJavaSparkContext(JavaRDD<?> anyRDD) {
		return JavaSparkContext.fromSparkContext(anyRDD.context());
	}

}
