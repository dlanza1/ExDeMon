package ch.cern.spark;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

public class StringUtilsTest {
    
    @Test
    public void getLastCharacter(){
        assertEquals(Optional.of('4'), StringUtils.getLastCharacter("1234"));
        assertEquals(Optional.of('m'), StringUtils.getLastCharacter("1234 m"));
        assertEquals(Optional.of(' '), StringUtils.getLastCharacter("1234 "));
        assertEquals(Optional.empty(), StringUtils.getLastCharacter(""));
    }
    
}
