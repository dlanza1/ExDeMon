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
    
}
