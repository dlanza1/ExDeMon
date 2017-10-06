package ch.cern.spark;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.StringUtils;

public class StringUtilsTest {

    @Test
    public void stringWithUnitToSeconds(){
        Assert.assertEquals(3, StringUtils.parseStringWithTimeUnitToSeconds("3"));
        Assert.assertEquals(23, StringUtils.parseStringWithTimeUnitToSeconds("23"));
        Assert.assertEquals(34, StringUtils.parseStringWithTimeUnitToSeconds("34s"));
        Assert.assertEquals(15 * 60, StringUtils.parseStringWithTimeUnitToSeconds("15m"));
        Assert.assertEquals(2 * 60 * 60, StringUtils.parseStringWithTimeUnitToSeconds("2h"));
    }
    
    @Test
    public void secondsToString(){
        Assert.assertEquals("0 seconds", StringUtils.secondsToString(0));
        Assert.assertEquals("1 second", StringUtils.secondsToString(1));
        Assert.assertEquals("3 seconds", StringUtils.secondsToString(3));
        Assert.assertEquals("1 minute", StringUtils.secondsToString(60));
        Assert.assertEquals("2 minutes", StringUtils.secondsToString(120));
        Assert.assertEquals("2 minutes and 5 seconds", StringUtils.secondsToString(125));
        Assert.assertEquals("1 hour", StringUtils.secondsToString(3600));
        Assert.assertEquals("2 hours", StringUtils.secondsToString(2 * 3600));
        Assert.assertEquals("2 hours and 1 minute", StringUtils.secondsToString(2 * 3600 + 60));
        Assert.assertEquals("2 hours and 6 seconds", StringUtils.secondsToString(2 * 3600 + 6));
        Assert.assertEquals("2 hours, 2 minutes and 5 seconds", StringUtils.secondsToString(2 * 3600 + 125));
    }
    
}
