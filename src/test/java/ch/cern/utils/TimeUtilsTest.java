package ch.cern.utils;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;

public class TimeUtilsTest {

    @Test
    public void stringWithUnitToSeconds() throws ConfigurationException{
        Assert.assertEquals(3, TimeUtils.parsePeriod("3").getSeconds());
        Assert.assertEquals(-23, TimeUtils.parsePeriod("-23").getSeconds());
        Assert.assertEquals(-34, TimeUtils.parsePeriod("-34s").getSeconds());
        Assert.assertEquals(15 * 60, TimeUtils.parsePeriod("15m").getSeconds());
        Assert.assertEquals(2 * 60 * 60, TimeUtils.parsePeriod("2h").getSeconds());
    }
    
    @Test
    public void durationToString(){
        Assert.assertEquals("0 seconds", TimeUtils.toString(Duration.ofSeconds(0)));
        Assert.assertEquals("1 second", TimeUtils.toString(Duration.ofSeconds(1)));
        Assert.assertEquals("3 seconds", TimeUtils.toString(Duration.ofSeconds(3)));
        Assert.assertEquals("1 minute", TimeUtils.toString(Duration.ofSeconds(60)));
        Assert.assertEquals("2 minutes", TimeUtils.toString(Duration.ofSeconds(120)));
        Assert.assertEquals("2 minutes and 5 seconds", TimeUtils.toString(Duration.ofSeconds(125)));
        Assert.assertEquals("1 hour", TimeUtils.toString(Duration.ofHours(1)));
        Assert.assertEquals("2 hours", TimeUtils.toString(Duration.ofHours(2)));
        Assert.assertEquals("2 hours and 1 minute", TimeUtils.toString(Duration.ofSeconds(2 * 3600 + 60)));
        Assert.assertEquals("2 hours and 6 seconds", TimeUtils.toString(Duration.ofSeconds(2 * 3600 + 6)));
        Assert.assertEquals("2 hours, 2 minutes and 5 seconds", TimeUtils.toString(Duration.ofSeconds(2 * 3600 + 125)));
    }
    
}
