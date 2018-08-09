package ch.cern.exdemon.monitor.analysis.types;

import java.io.IOException;
import java.time.Instant;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.numenta.nupic.network.PersistenceAPI;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.types.htm.MetricsFromFileReader;
import ch.cern.exdemon.monitor.analysis.types.htm.ResultsToFileWriter;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;

public class HTMAnalysisTest {
	
	private static final String DATA_FILE = "/home/fabio/Documents/htm.javaExperiments/data/art_daily_flatmiddle.csv";
	private static final String OUTPUT_DATA_FILE = "/home/fabio/Documents/htm.javaExperiments/data/output_art_daily_flatmiddle.csv";
	private static final String CSV_SPLIT_BY = ",";
	private static final String TIME_FORMAT = "YYYY-MM-dd HH:mm:ss";
	
	JSONStatusSerializer serializer = new JSONStatusSerializer();

	@Test
	public void serializeNetworkTest() throws ConfigurationException, IOException {
		HTMAnalysis htm = new HTMAnalysis();
		Random r = new Random();
		
		Properties properties = new Properties();
		properties.put(HTMAnalysis.TIMESTAMP_FORMAT, "YYYY-MM-dd'T'HH:mm:ss.SSSZ");
		
		htm.config(properties); 
		htm.load(null);
		htm.process(Instant.now(), r.nextDouble());
		StatusValue status = htm.save();
		byte[] barray = serializer.fromValue(status);
		for(int i = 0; i<50; i++) {
			System.out.println(i);
			
			long startTime = System.currentTimeMillis();
			status = serializer.toValue(barray);
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			System.out.println("toValue: "+elapsedTime);
			
			htm.load(status);
			htm.process(Instant.now(), r.nextDouble());
			status = htm.save();
			
			startTime = System.currentTimeMillis();
			barray = serializer.fromValue(status);
			stopTime = System.currentTimeMillis();
			elapsedTime = stopTime - startTime;
			System.out.println("fromValue: "+elapsedTime);
		}
	}
	
	@Test
	public void processTest() throws ConfigurationException{
		HTMAnalysis htm = new HTMAnalysis();
		Properties prop = new Properties();
		prop.put(HTMAnalysis.MAX_VALUE_PARAMS, 90);
		prop.put(HTMAnalysis.MIN_VALUE_PARAMS, -30);
		htm.config(prop);
		
		MetricsFromFileReader reader = new MetricsFromFileReader(DATA_FILE, CSV_SPLIT_BY, TIME_FORMAT);
		ResultsToFileWriter writer = new ResultsToFileWriter(OUTPUT_DATA_FILE);
		Metric metric;
		
		writer.writeHeader();
		reader.skipHeader();
		while (reader.hasNext()) {
			metric = reader.next();
			AnalysisResult results = htm.process(metric.getTimestamp(), metric.getValue().getAsFloat().get());
			Assert.assertNotEquals(AnalysisResult.Status.EXCEPTION, results.getStatus());
			
			Assert.assertTrue("checking if the network has calculated a possible good anomaly likelihood", 
					(double) results.getAnalysisParams().get("anomaly.likelihood") >= 0.0 
					&& (double) results.getAnalysisParams().get("anomaly.likelihood") <= 1.0);
			
			Assert.assertTrue("checking if the network has calculated the anomaly score", 
					(double) results.getAnalysisParams().get("anomaly.score") >= 0.0
					&& (double) results.getAnalysisParams().get("anomaly.score") <= 1.0);
			
			writer.write(results);
			
		}
		writer.close();

	}
}
