package ch.cern.spark.metrics.store;

public class TestStore implements Store {

	private static final long serialVersionUID = 2051099937848011407L;
	
	long number = 10;

	public TestStore(int i) {
		number = i;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (number ^ (number >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestStore other = (TestStore) obj;
		if (number != other.number)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Store_ [number=" + number + "]";
	}

}