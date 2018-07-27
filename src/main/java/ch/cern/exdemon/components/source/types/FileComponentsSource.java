package ch.cern.exdemon.components.source.types;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.components.source.ComponentsSource;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

@RegisterComponentType("file")
public class FileComponentsSource extends ComponentsSource {
    
    private transient final static Logger LOG = Logger.getLogger(FileComponentsSource.class.getName());

	private static final long serialVersionUID = -2444021351363428469L;
	
	private String path;

    private transient FileSystem fs;
    
    private Duration refreshPeriod = null;
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    @Override
    public ConfigurationResult configure(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        path = properties.getProperty("path");
        
        try {
            fs = FileSystem.get(new Configuration());
            if (path.startsWith("file:/"))
                fs = FileSystem.getLocal(new Configuration()).getRawFileSystem();
        } catch (IOException e) {
            return confResult.withError("path", e);
        }
        
        try {
            refreshPeriod = properties.getPeriod("expire", Duration.ofMinutes(5));
        } catch (ConfigurationException e) {
            return confResult.withError("expire", e);
        }
        
        return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
    public void initialize() throws Exception {
        final Runnable beeper = new Runnable() {
            public void run() { 
                try {
                    synchronize();
                } catch (Exception e) {
                    LOG.error("Error synchronizing components", e);
                }
            }
        };
        
        scheduler.scheduleAtFixedRate(beeper, 0, refreshPeriod.getSeconds(), TimeUnit.SECONDS);
    }

    private void synchronize() throws Exception {
        Properties newProperties = loadProperties();
        
        synchronize(Type.SCHEMA, newProperties.getSubset("metrics.schema"));
        synchronize(Type.METRIC, newProperties.getSubset("metrics.define"));
        synchronize(Type.MONITOR, newProperties.getSubset("monitors"));
        synchronize(Type.ACTUATOR, newProperties.getSubset("actuators"));
    }
    
    private void synchronize(Type componentType, Properties properties) {
        Set<String> existingIds = ComponentsCatalog.get(componentType).keySet();
        Set<String> newIds = properties.getIDs();
        
        // Remove component that are not in the new properties
        existingIds.stream().forEach(existingId -> {
            if(!newIds.contains(existingId))
                remove(componentType, existingId);
        });
        
        // Add/update components
        newIds.stream().forEach(id -> {
            Properties componentProps = properties.getSubset(id);
            
            register(componentType, id, componentProps);
        });
    }

    public Properties loadProperties() throws Exception {
        Properties props = new Properties();
        
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(path), true);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            
            if(fileStatus.isFile()) {
                String filePath = fileStatus.getPath().toString();
                
                LOG.info("Reading " + filePath);
                
                try {
                    Properties fileProps = Properties.fromFile(filePath);
                    
                    props.putAll(fileProps);
                    
                    LOG.info("Loaded " + filePath);
                }catch(Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        
        return props;
    }

    @Override
    public void close() {
        scheduler.shutdown();
    }
}
