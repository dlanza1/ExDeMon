package ch.cern.spark.metrics.value;
import java.io.Serializable;
import java.util.Optional;

public abstract class Value implements Serializable {

	private static final long serialVersionUID = -5082571575744839753L;
	
	protected String source;

	public Optional<Float> getAsFloat() {
		return Optional.empty();
	}
	
	public Optional<String> getAsString() {
		return Optional.empty();
	}
	
	public Optional<Boolean> getAsBoolean() {
		return Optional.empty();
	}
	
	public Optional<String> getAsException() {
		return Optional.empty();
	}
	
	public void setSource(String source) {
		this.source = source;
	}

	public String getSource() {
		return this.source;
	}
	
	@Override
	public String toString() {
		return source;
	}

}
