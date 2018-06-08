package ch.cern.utils;

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
    
    @Test
    public void countLines(){
        assertEquals(1, StringUtils.countLines("1234"));
        assertEquals(1, StringUtils.countLines("1234\n"));
        assertEquals(2, StringUtils.countLines("1234\n5678"));
        assertEquals(2, StringUtils.countLines("1234\n5678\n"));
    }
    
}
