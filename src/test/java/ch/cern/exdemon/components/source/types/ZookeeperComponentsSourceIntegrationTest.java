package ch.cern.exdemon.components.source.types;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;

public class ZookeeperComponentsSourceIntegrationTest {
    
    private TestingServer zkTestServer;
    private ZookeeperComponentsSource source;
    private Properties sourceProperties;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
        
        source = new ZookeeperComponentsSource();
        sourceProperties = new Properties();
    }
    
    @Test
    public void initialize() throws Exception {
        sourceProperties.setProperty("connection_string", "localhost:2182/");
        source.config(sourceProperties);

        source.initialize();
    }
    
    @Test(expected=IOException.class)
    public void noRootPath() throws Exception {
        sourceProperties.setProperty("connection_string", "localhost:2182/does-not-exist");
        source.config(sourceProperties);

        source.initialize();
    }
    
    @Test(expected=IOException.class)
    public void notAbleToConnect() throws Exception {
        sourceProperties.setProperty("connection_string", "othermachine:2182/");
        source.config(sourceProperties);

        source.initialize();
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        zkTestServer.close();
    }
    
}
