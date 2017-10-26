package ch.cern.utils;

public class IString {
	
	private String string;
	
	private int index;
	
	public IString(String input) {
		this.string = input;
		index = 0;
	}
	
	public boolean isLastChar() {
		return string.length() - 1 == index;
	}

	public char getChar() {
		return string.charAt(index);
	}

	public Character nextChar() {
		if(index + 1 < string.length()) {
			index++;
			
			return string.charAt(index);
		}else 
			return null;
	}

	public boolean startsWith(String prefix) {
		return string.startsWith(prefix, index);
	}

	public IString moveIndex(int diff) {
		index += diff;
		
		return this;
	}

	public boolean isFinished() {
		return index + 1 >= string.length();
	}

	public String getString() {
		return string;
	}

	public int getIndex() {
		return index;
	}
	
	@Override
	public String toString() {
		return string + " pos:" + index;
	}
	
}