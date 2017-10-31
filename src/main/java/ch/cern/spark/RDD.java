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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ch.cern.properties.ConfigurationException;

public class RDD <T> {
	
	public static final String CHECKPPOINT_DIR_PARAM = "spark.streaming.rdd.checkpoint.directory";

	private JavaRDD<T> rdd;
	
	private static transient FileSystem fs = null;
	
	private RDD(JavaRDD<T> rdd) {
		this.rdd = rdd;
	}
	
	public static <T> RDD<T> from(JavaRDD<T> rdd) {
		return new RDD<T>(rdd);
	}

	public JavaRDD<T> asJavaRDD(){
		return rdd;
	}
	
	public void save(String id) throws IllegalArgumentException, IOException, ConfigurationException {
		save(getStoringFile(getJavaSparkContext(), id), rdd.collect());
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
    
	public static<T> JavaRDD<T> load(JavaSparkContext context, String id)
			throws IOException, ClassNotFoundException, ConfigurationException {
		
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

	private static Path getStoringFile(JavaSparkContext context, String id) throws ConfigurationException {
		SparkConf sparkConf = context.getConf();
		
		String checkpointPathString = sparkConf.get(CHECKPPOINT_DIR_PARAM);
		if(checkpointPathString == null)
			throw new ConfigurationException(CHECKPPOINT_DIR_PARAM + " must be defined to store a RDD.");
		
		Path checkpointPath = new Path(checkpointPathString);
		
		return checkpointPath.suffix("/" + id).suffix("/latest");
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return JavaSparkContext.fromSparkContext(rdd.context());
	}
	
	private static void setFileSystem() throws IOException {
		if (fs == null)
			fs = FileSystem.get(new Configuration());
	}
	
}
