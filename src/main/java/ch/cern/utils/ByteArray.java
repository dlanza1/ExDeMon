package ch.cern.utils;

import java.io.Serializable;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=false)
public class ByteArray implements Serializable {

	private static final long serialVersionUID = -337388419815576498L;
	
	private byte[] values;

	public ByteArray(byte[] values) {
		super();
		this.values = values;
	}

	public byte[] get() {
		return values;
	}

	@Override
	public String toString() {
		return values != null ? new String(values) : "null";
	}
	
}
