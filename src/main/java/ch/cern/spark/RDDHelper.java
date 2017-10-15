package ch.cern.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDHelper {

	private static transient FileSystem fs = null;

	protected static <T> List<T> load(String storing_path) throws IOException, ClassNotFoundException {
		setFileSystem();

		Path finalFile = getStoringFile(storing_path);
		Path tmpFile = finalFile.suffix(".tmp");

		if (!fs.exists(finalFile) && fs.exists(tmpFile))
			fs.rename(tmpFile, finalFile);

		if (!fs.exists(finalFile))
			return new LinkedList<T>();

		ObjectInputStream is = new ObjectInputStream(fs.open(finalFile));

		@SuppressWarnings("unchecked")
		List<T> elements = (List<T>) is.readObject();

		is.close();

		return elements;
	}

	public static<T> JavaRDD<T> load(String storing_path, JavaSparkContext context)
			throws IOException, ClassNotFoundException {
		return context.parallelize(load(storing_path));
	}

	public static<T> void save(JavaRDD<T> input, String storing_path) throws IllegalArgumentException, IOException {
		save(storing_path, input.collect());
	}

	protected static void save(String storing_path, List<?> elements) throws IllegalArgumentException, IOException {
		setFileSystem();

		Path finalFile = getStoringFile(storing_path);
		Path tmpFile = finalFile.suffix(".tmp");

		fs.mkdirs(tmpFile.getParent());

		ObjectOutputStream oos = new ObjectOutputStream(fs.create(tmpFile, true));
		oos.writeObject(elements);
		oos.close();

		if (canBeRead(tmpFile)) {
			fs.delete(finalFile, false);
			fs.rename(tmpFile, finalFile);
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

	private static Path getStoringFile(String storing_path) {
		return new Path(storing_path + "/metricStores/latest");
	}

}
