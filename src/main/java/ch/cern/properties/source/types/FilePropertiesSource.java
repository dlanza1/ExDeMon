package ch.cern.properties.source.types;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;

@RegisterComponent("file")
public class FilePropertiesSource extends PropertiesSource {
    
    private transient final static Logger LOG = Logger.getLogger(FilePropertiesSource.class.getName());

	private static final long serialVersionUID = -2444021351363428469L;
	
	private String path;

    private transient FileSystem fs;
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
        path = properties.getProperty("path");
        
        properties.confirmAllPropertiesUsed();
        
        try {
            fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public Properties load() throws Exception {
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
                    LOG.error(e);
                }
            }
        }
        
        return props;
    }

}
