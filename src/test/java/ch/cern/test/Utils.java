package ch.cern.test;

import static org.junit.Assert.fail;

import java.util.Optional;

public class Utils {

	public static void assertNotPresent(Optional<?> optional) {
		if(optional.isPresent())
			fail();
	}
	
	public static void assertPresent(Optional<?> optional) {
		if(!optional.isPresent())
			fail();
	}
	
}
