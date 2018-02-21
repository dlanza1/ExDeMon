package ch.cern.properties;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ch.cern.Cache;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.spark.json.JSONParser;
import ch.cern.utils.Pair;
import ch.cern.utils.TimeUtils;
import scala.Tuple2;

public class Properties extends java.util.Properties {

    private static transient final long serialVersionUID = 2510326766802151233L;

    private transient final static Logger LOG = Logger.getLogger(Properties.class.getName());

    private static Cache<Properties> cachedProperties = null;

    private static Pattern ID_REGEX = Pattern.compile("[a-zA-Z0-9_-]+");

    private Set<String> usedKeys = new HashSet<>();

    public Properties() {
    }

    public Properties(Properties props) {
        putAll(props);
    }

    public static Properties fromFile(String loadingPath) throws IOException {
        Properties props = null;
        
        FileSystem fs = FileSystem.get(new Configuration());
        if (loadingPath.startsWith("file:/"))
            fs = FileSystem.getLocal(new Configuration()).getRawFileSystem();
        
        InputStreamReader is = new InputStreamReader(fs.open(new Path(loadingPath)));
        
        String possibleJSON = IOUtils.toString(is);
        is.close();
        
        if(JSONParser.isValid(possibleJSON)) {
            JsonObject jsonObject = new JsonParser().parse(possibleJSON).getAsJsonObject();
            
            props = from(jsonObject);
        }else {
            props = new Properties();
            
            is = new InputStreamReader(fs.open(new Path(loadingPath)));
            
            props.load(is);
            
            is.close();
        }

        return props;
    }
    
    public static Properties fromJson(String possibleJSON) {
        Properties props = null;
        
        if(JSONParser.isValid(possibleJSON)) {
            JsonObject jsonObject = new JsonParser().parse(possibleJSON).getAsJsonObject();
            props = from(jsonObject);
        }
        
        return props;
    }

    public List<String> getKeysThatStartWith(String prefix) {
        return keySet().stream().map(String::valueOf).filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
    }

    @Override
    public String getProperty(String key) {
        usedKeys.add(key);

        return super.getProperty(key);
    }

    public Properties getSubset(String topLevelKey) {
        topLevelKey += ".";

        Properties properties = new Properties();

        List<String> keysWithPrefix = getKeysThatStartWith(topLevelKey);

        for (String keyWithPrefix : keysWithPrefix) {
            String keyWithoutPrefix = keyWithPrefix.substring(topLevelKey.length());

            properties.setProperty(keyWithoutPrefix, getProperty(keyWithPrefix));
        }

        return properties;
    }

    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);

        return value != null ? value : defaultValue;
    }

    public Set<String> getIDs() {
        return keySet().stream().map(String::valueOf).map(s -> s.split("\\.")).filter(spl -> spl.length > 0)
                .map(spl -> spl[0]).filter(id -> ID_REGEX.matcher(id).matches()).collect(Collectors.toSet());
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

    public long getLong(String key, long defaultValue) {
        Long value = getLong(key);

        return value == null ? defaultValue : value;
    }

    public boolean getBoolean(String key) throws ConfigurationException {
        return getBoolean(key, false);
    }
    
    public boolean getBoolean(String key, boolean defaultValue) throws ConfigurationException {
        String value = getProperty(key);

        if (value == null)
            return defaultValue;

        if (value.toLowerCase().equals("true"))
            return true;
        else if (value.toLowerCase().equals("false"))
            return false;
        else
            throw new ConfigurationException(
                    key + " expects boolean value (true or false). \"" + value + "\" could not be parsed");
    }

    public void setPropertyIfAbsent(String key, String value) {
        if (!containsKey(key))
            setProperty(key, value);
    }
    
    private void addProperties(String subfix, Properties newProps) {
        for (Map.Entry<Object, Object> item : newProps.entrySet())
            put(subfix + "." + item.getKey(), item.getValue());
    }

    public boolean isTypeDefined() {
        return getProperty("type") != null;
    }

    public Duration getPeriod(String key, Duration periodDefault) throws ConfigurationException {
        String value = getProperty(key);

        if (value == null)
            return periodDefault;

        try {
            return TimeUtils.parsePeriod(value);
        } catch (NumberFormatException e) {
            throw new ConfigurationException("For key=" + key + ": " + e.getMessage());
        }
    }

    public Optional<Duration> getPeriod(String key) throws ConfigurationException {
        return Optional.ofNullable(getPeriod(key, null));
    }

    public static Cache<Properties> getCache() {
        return cachedProperties;
    }

    public static void resetCache() {
        Properties.cachedProperties = null;
    }

    public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
        if (Properties.cachedProperties == null)
            Properties.cachedProperties = new PropertiesCache(propertiesSourceProps);

        if (propertiesSourceProps != null)
            getCache().setExpiration(propertiesSourceProps.getPeriod("expire", Duration.ofMinutes(1)));
    }

    public void setDefaultPropertiesSource(String propertyFilePath) {
        Properties propertiesSourceProperties = getSubset(PropertiesSource.CONFIGURATION_PREFIX);

        if (!propertiesSourceProperties.containsKey("type")) {
            setProperty(PropertiesSource.CONFIGURATION_PREFIX + ".type", "file");
            setProperty(PropertiesSource.CONFIGURATION_PREFIX + ".path", propertyFilePath);
        }
    }

    public static class PropertiesCache extends Cache<Properties> {

        private Properties propertiesSourceProps;

        public PropertiesCache(Properties propertiesSourceProps) {
            this.propertiesSourceProps = propertiesSourceProps;
        }

        @Override
        protected Properties load() throws Exception {
            LOG.info("Loading properties.");
            LOG.info("Properties source parameters: " + propertiesSourceProps);

            if (propertiesSourceProps == null)
                return new Properties();

            Optional<PropertiesSource> propertiesSource = ComponentManager.buildOptional(Type.PROPERTIES_SOURCE, propertiesSourceProps);

            if (propertiesSource.isPresent()) {
                Properties properties = propertiesSource.get().load();

                LOG.info("Properties loaded from source.");

                return properties;
            }else {
                LOG.info("Properties source not configured.");
                
                return propertiesSourceProps;
            }
        }
        
        @Override
        protected boolean inErrorGetPrevious() {
            return true;
        }

    }

    public void confirmAllPropertiesUsed() throws ConfigurationException {
        HashSet<Object> leftKeys = new HashSet<>(keySet());
        leftKeys.removeAll(usedKeys);

        if (!leftKeys.isEmpty())
            throw new ConfigurationException("Some configuration parameters (" + leftKeys + ") were not used.");
    }

    public Map<String, String> toStringMap() {
        return entrySet().stream().map(e -> new Pair<String, String>(e.getKey().toString(), e.getValue().toString()))
                .collect(Collectors.toMap(Pair::first, Pair::second));
    }

    public static Properties from(Tuple2<String, String>[] values) {
        Properties properties = new Properties();

        for (Tuple2<String, String> value : values)
            properties.setProperty(value._1, value._2);

        return properties;
    }

    public static Properties from(JsonObject json) {
        Properties properties = new Properties();

        for (Map.Entry<String, JsonElement> item : json.entrySet())
            if(item.getValue().isJsonPrimitive())
                properties.setProperty(item.getKey(), item.getValue().getAsJsonPrimitive().getAsString());
            else if(item.getValue().isJsonObject())
                properties.addProperties(item.getKey(), from(item.getValue().getAsJsonObject()));
        
        return properties;
    }
    
    public void addFrom(JsonObject json) {
        for (Map.Entry<String, JsonElement> item : json.entrySet())
            if(item.getValue().isJsonPrimitive())
                this.setProperty(item.getKey(), item.getValue().getAsJsonPrimitive().getAsString());
            else if(item.getValue().isJsonObject())
                this.addProperties(item.getKey(), from(item.getValue().getAsJsonObject()));
    }

}
