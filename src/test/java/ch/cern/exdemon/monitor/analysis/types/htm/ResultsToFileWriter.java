package ch.cern.exdemon.monitor.analysis.types.htm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import com.esotericsoftware.minlog.Log;

import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;

public class ResultsToFileWriter {
	//private String filename;
	PrintWriter writer;
	
	public ResultsToFileWriter(String filename) {
		try {
			this.writer = new PrintWriter(filename);
		} catch (FileNotFoundException e) {
			Log.error("can't open the file: "+filename);
			e.printStackTrace();
		}
	}
	
	public void write(AnalysisResult results) {
		StringBuilder s = new StringBuilder();
		s.append(results.getAnalysis_timestamp()).append(",")
			.append(results.getAnalysisParams().get("anomaly.likelihood")).append(",")
			.append(results.getAnalysisParams().get("anomaly.score")).append(",")
			.append(results.getStatus() == AnalysisResult.Status.WARNING ? 1 : 0).append(",")
			.append(results.getStatus() == AnalysisResult.Status.ERROR ? 1 : 0);
		writer.println(s.toString());
		writer.flush();
	}
	
	public void writeHeader() {
		writer.println(getHeader());
	}
	
	public String getHeader() {
		StringBuilder header = new StringBuilder();
		header.append("timestamp").append(",")
			.append("anomaly.likelihood").append(",")
			.append("anomaly.score").append(",")
			.append("warning").append(",")
			.append("error");
		return header.toString();
	}
	
	public void close() {
		writer.close();
	}
}
