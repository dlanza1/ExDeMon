package ch.cern.exdemon.monitor.analysis.types;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.net.util.Base64;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.types.htm.MetricsFromFileReader;
import ch.cern.exdemon.monitor.analysis.types.htm.ResultsToFileWriter;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;

public class HTMAnalysisTest {
	
	private static final String DATA_FILE = "src/test/resources/write_mounts.csv";
	private static final String OUTPUT_DATA_FILE = "src/test/resources/write_mounts_outputs.csv";
	private static final int EXPECTED_ERRORS = 7;
	private static final int EXPECTED_WARNINGS = 4;
	private static final String CSV_SPLIT_BY = ",";
	private static final String TIME_FORMAT = "YYYY-MM-dd'T'HH:mm:ssZ";
	
	StatusSerializer serializer = new JavaStatusSerializer();
	
	@Test
	public void serializeNetworkTest() throws ConfigurationException, IOException {

		
		HTMAnalysis htm = new HTMAnalysis();
		Properties prop = new Properties();
		prop.put(HTMAnalysis.MAX_VALUE_PARAMS, 200);
		prop.put(HTMAnalysis.MIN_VALUE_PARAMS, 0);
		
		htm.config(prop);
		
		MetricsFromFileReader reader = new MetricsFromFileReader(DATA_FILE, CSV_SPLIT_BY, TIME_FORMAT);
		ResultsToFileWriter writer = new ResultsToFileWriter(OUTPUT_DATA_FILE);
		int nErrors = 0;
		int nWarnings = 0;
		int i = 0;
		
		writer.writeHeader();
		reader.skipHeader();
		Metric metric = reader.next();
		
		htm.load(null);
		htm.process(metric.getTimestamp(), metric.getValue().getAsFloat().get());
		StatusValue status = htm.save();
		byte[] barray = serializer.fromValue(status);
		
		while(reader.hasNext()) {
			System.out.println(i);
			
			
			status = serializer.toValue(barray);
			htm.load(status);
			
			metric = reader.next();
			AnalysisResult results = htm.process(metric.getTimestamp(), metric.getValue().getAsFloat().get());
			if(results.getStatus() == AnalysisResult.Status.WARNING)
				nWarnings++;
			
			if(results.getStatus() == AnalysisResult.Status.ERROR)
				nErrors++;
			
			writer.write(results);
			status = htm.save();
			barray = serializer.fromValue(status);
			i++;
		}
		
		System.out.println(i);
		Assert.assertEquals(EXPECTED_WARNINGS, nWarnings);
		Assert.assertEquals(EXPECTED_ERRORS, nErrors);
		writer.close();
	}
	
	@Test
	public void processTest() throws ConfigurationException{
		HTMAnalysis htm = new HTMAnalysis();
		Properties prop = new Properties();
		prop.put(HTMAnalysis.MAX_VALUE_PARAMS, 200);
		prop.put(HTMAnalysis.MIN_VALUE_PARAMS, 0);
		htm.config(prop);
		
		MetricsFromFileReader reader = new MetricsFromFileReader(DATA_FILE, CSV_SPLIT_BY, TIME_FORMAT);
		ResultsToFileWriter writer = new ResultsToFileWriter(OUTPUT_DATA_FILE);
		Metric metric;
		
		int nErrors = 0;
		int nWarnings = 0;
		int i = 0;
		
		StatusValue status = null;
		
		writer.writeHeader();
		reader.skipHeader();
		while (reader.hasNext()) {
			
			metric = reader.next();
			htm.load(status);
			AnalysisResult results = htm.process(metric.getTimestamp(), metric.getValue().getAsFloat().get());
			status = htm.save();
			
			
			Assert.assertNotEquals(AnalysisResult.Status.EXCEPTION, results.getStatus());
			if(results.getStatus() == AnalysisResult.Status.WARNING)
				nWarnings++;
			
			if(results.getStatus() == AnalysisResult.Status.ERROR)
				nErrors++;
			
			
//			Assert.assertTrue("checking if the network has calculated a possible good anomaly likelihood", 
//					(double) results.getAnalysisParams().get("anomaly.likelihood") >= 0.0 
//					&& (double) results.getAnalysisParams().get("anomaly.likelihood") <= 1.0);
			
//			Assert.assertTrue("checking if the network has calculated the anomaly score", 
//					(double) results.getAnalysisParams().get("anomaly.score") >= 0.0
//					&& (double) results.getAnalysisParams().get("anomaly.score") <= 1.0);
			
			writer.write(results);
			
			byte[] barray;
			try {
				barray = serializer.fromValue(status);
				status = serializer.toValue(barray);
				((HTMAnalysis.Status_)status).network.postDeSerialize();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(i);
			i++;
		}
		System.out.println(i);
		Assert.assertEquals(EXPECTED_WARNINGS, nWarnings);
		Assert.assertEquals(EXPECTED_ERRORS, nErrors);
		writer.close();
	}
	
	@Test
	public void Base64Test(){
		byte[] barrayA = {'a', 'b', 'c'};
		byte[] barrayB;
		String a = Base64.encodeBase64String(barrayA);
		
		barrayB = Base64.decodeBase64(a);
		
		for(byte b: barrayA)
			System.out.print(b+" ");
		System.out.println();
		
		for(byte b: barrayB)
			System.out.print(b+" ");
		System.out.println();
		
		Assert.assertTrue(Arrays.equals(barrayA, barrayB));
	}
}
