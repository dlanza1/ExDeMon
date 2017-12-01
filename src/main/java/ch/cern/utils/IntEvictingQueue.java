package ch.cern.utils;

public class IntEvictingQueue {

	private int[] values;
	
	private int head;
	
	private int size;
	
	public IntEvictingQueue(int limit) {
		values = new int[limit];
		head = 0;
		size = 0;
	}

	public void add(int newValue) {
		head++;
		if(head == values.length)
			head = 0;
		
		values[head] = newValue;
		
		if(size < values.length)
			size++;
	}

	public int size() {
		return size;
	}

	public int get(int i) {
		if(i >= size || i < 0)
			throw new IndexOutOfBoundsException(i + " is >= size (" + size + ") or < 0");
		
		return values[(size - head + 1 + i) % values.length];
	}

}
