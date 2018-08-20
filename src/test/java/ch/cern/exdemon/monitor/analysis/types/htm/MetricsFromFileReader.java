package ch.cern.exdemon.monitor.analysis.types.htm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.esotericsoftware.minlog.Log;

import ch.cern.exdemon.metrics.Metric;

public class MetricsFromFileReader implements Iterator<Metric> {
	
	private FileReader fr;
	private BufferedReader br;
	private String nextString = null;
	private String splittingString;
	private String timeformat;

	public MetricsFromFileReader(String filename, String splittingString, String timeformat) {
		this.splittingString = splittingString;
		this.timeformat = timeformat;
		
		
		try {
			fr = new FileReader(filename);
			br = new BufferedReader(fr);
			
			nextString = br.readLine(); 
            if( nextString == null )
            {
                br.close();
                br = null;
            }
			
		} catch (FileNotFoundException e) {
			Log.error("Can't read the file: "+filename);
			e.printStackTrace();
		} catch (IOException e) {
			Log.error("Can't read line");
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean hasNext() {
		return nextString != null;
	}

	@Override
	public Metric next() {
		Metric metric = getMetric();
 
		readNextLine();
        
        return metric;
	}

	private void readNextLine() {
		try
        {
            if( nextString == null )
            {
                throw new NoSuchElementException( "Next line is not available" );
            }
            else
            {
            	nextString = br.readLine();
                if( nextString == null && br != null )
                {
                    br.close();
                    br = null;
                }
            }
        }
        catch( Exception ex )
        {
            throw new NoSuchElementException( "Exception caught in FileLineIterator.next() " + ex );
        }
	}
	
	private Metric getMetric() {
		String[] splittedRow = nextString.split(splittingString);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(timeformat);
        DateTime dt = formatter.parseDateTime(splittedRow[0]);
        Instant instant = java.time.Instant.ofEpochMilli(dt.toInstant().getMillis());
        Float value = Float.parseFloat(splittedRow[1]);
        return new Metric(instant, value, new HashMap<String, String>());
	}
	
    @Override
    protected void finalize()
    throws Throwable
    {
        try
        {
            nextString = null;
            if( br != null ) try{ br.close(); } catch( Exception ex ) { }
            br = null;
        }
        finally
        {
            super.finalize();
        }
    }

	public void skipHeader() {
		readNextLine();
		readNextLine();
		readNextLine();
	}
	

}
