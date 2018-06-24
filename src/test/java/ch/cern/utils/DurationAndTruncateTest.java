package ch.cern.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;

public class DurationAndTruncateTest {
    
    @Test
    public void fromConfig() throws ConfigurationException {
        DurationAndTruncate durAndRound = DurationAndTruncate.from("15m");
        assertEquals(Duration.ofMinutes(15), durAndRound.getDuration());
        assertEquals(ChronoUnit.NANOS, durAndRound.getTruncate());
        
        durAndRound = DurationAndTruncate.from("1m,m");
        assertEquals(Duration.ofMinutes(1), durAndRound.getDuration());
        assertEquals(ChronoUnit.MINUTES, durAndRound.getTruncate());
        
        durAndRound = DurationAndTruncate.from("1m,h");
        assertEquals(Duration.ofMinutes(1), durAndRound.getDuration());
        assertEquals(ChronoUnit.HOURS, durAndRound.getTruncate());
        
        durAndRound = DurationAndTruncate.from("1m,d");
        assertEquals(Duration.ofMinutes(1), durAndRound.getDuration());
        assertEquals(ChronoUnit.DAYS, durAndRound.getTruncate());
        
        try {
            durAndRound = DurationAndTruncate.from("1m,n");
            
            fail();
        }catch(ConfigurationException e) {
            assertEquals("Truncation n not available.", e.getMessage());
        }
    }
    
    @Test
    public void shouldApplyDurationOnly() throws ConfigurationException {
        DurationAndTruncate durAndRound = DurationAndTruncate.from("1m");
        
        assertEquals(Instant.parse("2017-12-10T14:19:10Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:10Z")));
    }
    
    @Test
    public void shouldApplyDurationAndTruncate() throws ConfigurationException {
        DurationAndTruncate durAndRound = DurationAndTruncate.from("1m,m");
        assertEquals(Instant.parse("2017-12-10T14:19:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:00Z")));
        assertEquals(Instant.parse("2017-12-10T14:19:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:12Z")));
        assertEquals(Instant.parse("2017-12-10T14:19:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:59Z")));
        
        durAndRound = DurationAndTruncate.from("0,m");
        assertEquals(Instant.parse("2017-12-10T14:20:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:00Z")));
        assertEquals(Instant.parse("2017-12-10T14:20:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:12Z")));
        assertEquals(Instant.parse("2017-12-10T14:20:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:59Z")));

        durAndRound = DurationAndTruncate.from("1d,d");        
        assertEquals(Instant.parse("2017-12-09T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:00Z")));
        assertEquals(Instant.parse("2017-12-09T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:12Z")));
        assertEquals(Instant.parse("2017-12-09T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:59Z")));
        
        durAndRound = DurationAndTruncate.from("0d,d");        
        assertEquals(Instant.parse("2017-12-10T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:00Z")));
        assertEquals(Instant.parse("2017-12-10T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:12Z")));
        assertEquals(Instant.parse("2017-12-10T00:00:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:59Z")));
        
        durAndRound = DurationAndTruncate.from("2m,m");
        assertEquals(Instant.parse("2017-12-10T14:18:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:00Z")));
        assertEquals(Instant.parse("2017-12-10T14:18:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:12Z")));
        assertEquals(Instant.parse("2017-12-10T14:18:00Z"), durAndRound.adjustMinus(Instant.parse("2017-12-10T14:20:59Z")));
    }

}
