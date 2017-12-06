package ch.cern.spark.status.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public class JavaStatusSerializer implements StatusSerializer {

	private static final long serialVersionUID = -406547771533268077L;

	@Override
	public StatusKey toKey(byte[] bytes) throws IOException {
		return to(bytes);
	}

	@Override
	public byte[] fromKey(StatusKey key) throws IOException {
		return from(key);
	}
	
	@Override
	public StatusValue toValue(byte[] bytes) throws IOException {
		return to(bytes);
	}

	@Override
	public byte[] fromValue(StatusValue value) throws IOException {
		return from(value);
	}

	@SuppressWarnings("unchecked")
	public static<T> T to(byte[] bytes) throws IOException {
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(new ByteArrayInputStream(bytes));

			return (T) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {}
		}
	}

	public static<T> byte[] from(T key) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(key);
			out.flush();

			return bos.toByteArray();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {}
		}
	}

}
