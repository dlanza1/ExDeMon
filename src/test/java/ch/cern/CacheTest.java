package ch.cern;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;

public class CacheTest {
    
    @Before
    public void setUp() throws Exception {
        Properties.initCache(null);
        Properties.getCache().reset();
    }
    
    @Test
    public void hasExpired() throws Exception{
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.setExpiration(Duration.ofSeconds(1));
        
        Instant now = Instant.now();
        Properties p1 = propertiesCache.get();
        
        Properties p2 = propertiesCache.get();
        assertSame(p1, p2);
        
        assertTrue(propertiesCache.hasExpired(now.plus(Duration.ofSeconds(2))));
        
        propertiesCache.setExpiration(null);
    }
    
    @Test
    public void loadWhenCacheExpires() throws Exception{
        Cache<Properties> propertiesCache = spy(Properties.getCache());
        propertiesCache.setExpiration(Duration.ofSeconds(1));
        
        propertiesCache.get();
        //first load
        verify(propertiesCache, times(1)).loadCache(any());
        
        Thread.sleep(100);
        propertiesCache.get();
        //same first load
        verify(propertiesCache, times(1)).loadCache(any());
        
        Thread.sleep(1000);
        propertiesCache.get();
        //Has expired after 1100 ms, so load again
        verify(propertiesCache, times(2)).loadCache(any());
        
        propertiesCache.setExpiration(null);
    }

}
