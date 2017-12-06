package ch.cern.spark.status.storage;

import java.io.IOException;
import java.io.Serializable;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public interface StatusSerializer extends Serializable{
	
	public StatusKey toKey(byte[] bytes) throws IOException;
	
	public byte[] fromKey(StatusKey key) throws IOException;

	public StatusValue toValue(byte[] bytes) throws IOException;
	
	public byte[] fromValue(StatusValue value) throws IOException;

}
