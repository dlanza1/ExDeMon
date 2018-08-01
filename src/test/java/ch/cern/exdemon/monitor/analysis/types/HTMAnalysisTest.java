package ch.cern.exdemon.monitor.analysis.types;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
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
		//byte[] barray = serializer.fromValue(status);
		
		for(int i = 0; i<50; i++) {
			//status = serializer.toValue(barray);
			htm.load(status);
			htm.process(Instant.now(), r.nextDouble());
			status = htm.save();
			//barray = serializer.fromValue(status);
		}
	}
	
	@Test
	public void processTest() throws ConfigurationException{
		HTMAnalysis htm = new HTMAnalysis();
		Properties prop = new Properties();
		prop.put(HTMAnalysis.MAX_VALUE_PARAMS, 90);
		prop.put(HTMAnalysis.MIN_VALUE_PARAMS, -30);
		htm.config(prop);
		//htm.load(null);
		
		FileReader fr = null;
		BufferedReader br = null;
		PrintWriter writer = null;
		try {
			
			fr = new FileReader(DATA_FILE);
			writer = new PrintWriter(OUTPUT_DATA_FILE, "UTF-8");
			br = new BufferedReader(fr);
			String line;
			
			//reading header
			br.readLine();
			br.readLine();
			br.readLine();
			
			StringBuilder out_header = new StringBuilder();
			out_header.append("timestamp").append(",")
				.append("anomaly.likelihood").append(",")
				.append("anomaly.score").append(",")
				.append("warning").append(",")
				.append("error");
			writer.println(out_header);
			
			while ((line = br.readLine()) != null) {
				
				String[] splittedRow = line.split(CSV_SPLIT_BY);
				java.time.Instant timestamp = stringToInstant(splittedRow[0], TIME_FORMAT);
				double value = Double.parseDouble(splittedRow[1]);
				AnalysisResult results = htm.process(timestamp, value);
				Assert.assertNotEquals(AnalysisResult.Status.EXCEPTION, results.getStatus());
				
				Assert.assertTrue("checking if the network has calculated a possible good anomaly likelihood", 
						(double) results.getAnalysisParams().get("anomaly.likelihood") >= 0.0 
						&& (double) results.getAnalysisParams().get("anomaly.likelihood") <= 1.0);
				
				Assert.assertTrue("checking if the network has calculated the anomaly score", 
						(double) results.getAnalysisParams().get("anomaly.score") >= 0.0
						&& (double) results.getAnalysisParams().get("anomaly.score") <= 1.0);
				
				StringBuilder s = new StringBuilder();
				s.append(results.getTimestamp()).append(",")
					.append(results.getAnalysisParams().get("anomaly.likelihood")).append(",")
					.append(results.getAnalysisParams().get("anomaly.score")).append(",")
					.append(results.getStatus() == AnalysisResult.Status.WARNING ? 1 : 0).append(",")
					.append(results.getStatus() == AnalysisResult.Status.ERROR ? 1 : 0);
				writer.println(s.toString());
				
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();
				
				if (writer != null)
					writer.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}

	}
	
	private java.time.Instant stringToInstant(String s, String format) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        DateTime dt = formatter.parseDateTime(s);
        return java.time.Instant.ofEpochMilli(dt.toInstant().getMillis());
	}
	
	
	
}
