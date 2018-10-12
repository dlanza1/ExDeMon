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
    
    @Test
    public void headLines(){
        String input = "1\n"
                     + "2\n"
                     + "3\n"
                     + "4";
        
        String expected = "1\n"
                        + "2";
        
        assertEquals(expected, StringUtils.headLines(input, 2));
    }
    
    @Test
    public void headLinesLessLinesThanMax(){
        String input = "1\n"
                     + "2\n"
                     + "3\n"
                     + "4";

        assertEquals(input, StringUtils.headLines(input, 20));
    }
    
    @Test
    public void headLinesWithoutEOL(){
        String input = "12345";
        
        String expected = "12345";
        
        assertEquals(expected, StringUtils.headLines(input, 2));
    }
    
    @Test
    public void dataAmount(){
        assertEquals("2.0 bytes", StringUtils.asDataAmount(2));
        assertEquals("2.3 KB", StringUtils.asDataAmount(2300));
        assertEquals("56.31 MB", StringUtils.asDataAmount(56312123));
        assertEquals("52.35 GB", StringUtils.asDataAmount(52346312123l));
        assertEquals("9.84 TB", StringUtils.asDataAmount(9844335987594l));
    }
    
}
