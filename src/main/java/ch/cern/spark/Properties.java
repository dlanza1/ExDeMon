package ch.cern.spark;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Properties extends java.util.Properties{
	
	private static final long serialVersionUID = 2510326766802151233L;
	
	public static class Expirable extends ObjectExpirable<Properties> implements Serializable{
		private static final long serialVersionUID = -5361682529035003933L;
		
		private String path;
		
		public Expirable(String path) {
		    super(5);
		    
			this.path = path;
		}

		@Override
		protected Properties loadObject() throws IOException {
			Properties props = new Properties();
			
	        FileSystem fs = FileSystem.get(new Configuration());

	        InputStreamReader is = new InputStreamReader(fs.open(new Path(path)));
			
	        props.load(is);

			is.close();

			return props;
		}
	}
	
	public Properties() {
    }

    public List<String> getKeysThatStartWith(String prefix) {
        return keySet().stream()
                .map(String::valueOf)
                .filter(s -> s.startsWith(prefix))
                .collect(Collectors.toList());
    }
    
    public Properties getSubset(String topLevelKey){
        topLevelKey += ".";
        
        Properties properties = new Properties();
        
        List<String> keysWithPrefix = getKeysThatStartWith(topLevelKey);
        
        for (String keyWithPrefix : keysWithPrefix) {
            String keyWithoutPrefix = keyWithPrefix.replace(topLevelKey, "");
            
            properties.setProperty(keyWithoutPrefix, getProperty(keyWithPrefix));
        }
        
        return properties;
    }

    public Set<String> getUniqueKeyFields(int index) {
        Set<String> uniqueValues = new HashSet<>();
        
        for (Object key_object : keySet()) {
            String[] property_name_splits = ((String) key_object).split("\\.");

            if(property_name_splits.length >= index){
                String field = property_name_splits[index];
                
                uniqueValues.add(field);   
            }
        }
        
        return uniqueValues;
    }

    public Float getFloat(String key) {
        String value = getProperty(key);
        
        return value == null ? null : Float.parseFloat(value);
    }
    
    public float getFloat(String key, float defaultValue) {
        Float value = getFloat(key);
        
        return value == null ? defaultValue : value;
    }

    public Long getLong(String key) {
        String value = getProperty(key);
        
        return value == null ? null : Long.parseLong(value);
    }

    public boolean getBoolean(String key) {
        String value = getProperty(key);
        
        return value == null ? false : Boolean.parseBoolean(value);
    }
    
    public void setPropertyIfAbsent(String key, String value){
        if(!containsKey(key))
            setProperty(key, value);
    }

	public Optional<Duration> getPeriod(String key, Duration periodDefault) {
		return Optional.ofNullable(TimeUtils.parsePeriod(getProperty(key)).orElse(periodDefault));
	}

}
