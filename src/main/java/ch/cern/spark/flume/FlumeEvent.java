package ch.cern.spark.flume;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.source.avro.AvroFlumeEvent;

import com.google.common.base.Optional;

@SuppressWarnings("serial")
public class FlumeEvent implements Serializable{

	private Map<String, String> headers;
	private byte[] body;

	public FlumeEvent(Map<String, String> headers, byte[] body) {
		this.headers = headers;
		this.body = body;
	}

	@Override
	public String toString() {
		return "FlumeEvent [headers=" + headers + ", body=" + new String(body) + "]";
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public byte[] getBody() {
		return body;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof FlumeEvent))
			return false;
		
		FlumeEvent other = (FlumeEvent) obj;
		
		return headers.equals(other.headers) && Arrays.equals(body, other.body);
	}
	
	public static class Decoder implements Serializable{

		private Optional<SpecificDatumReader<AvroFlumeEvent>> reader;
		private BinaryDecoder decoder;
		
		public Decoder() {
			reader = Optional.absent();
		}
		
		public FlumeEvent decode(byte[] bytes) {
			ByteArrayInputStream in = new ByteArrayInputStream(bytes);
			
			decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);

			if(!reader.isPresent())
				reader = Optional.of(new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));

			try {
				AvroFlumeEvent avroFlumeEvent = reader.get().read(null, decoder);
				
				return new FlumeEvent(toStringMap(
						avroFlumeEvent.getHeaders()), 
						avroFlumeEvent.getBody().array());
			} catch (IOException e) {
				return null;
			}
		}
		
		private Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
			Map<String, String> stringMap = new HashMap<String, String>();
			
			for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet())
				stringMap.put(entry.getKey().toString(), entry.getValue().toString());
			
			return stringMap;
		}
		
		private void writeObject(java.io.ObjectOutputStream out) 
				throws IOException {	
		}
		
		private void readObject(java.io.ObjectInputStream in) 
				throws IOException, ClassNotFoundException {
			reader = Optional.absent();
		}
		
	}
	
}
