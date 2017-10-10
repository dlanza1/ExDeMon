package ch.cern.spark;

import org.junit.Assert;
import org.junit.Test;

public class TimeUtilsTest {

    @Test
    public void stringWithUnitToSeconds(){
        Assert.assertEquals(3, TimeUtils.parsePeriod("3").getSeconds());
        Assert.assertEquals(23, TimeUtils.parsePeriod("23").getSeconds());
        Assert.assertEquals(34, TimeUtils.parsePeriod("34s").getSeconds());
        Assert.assertEquals(15 * 60, TimeUtils.parsePeriod("15m").getSeconds());
        Assert.assertEquals(2 * 60 * 60, TimeUtils.parsePeriod("2h").getSeconds());
    }
    
}
