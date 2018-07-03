package ch.cern.exdemon.struct.filter;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.cern.exdemon.struct.Metric;
import ch.cern.exdemon.struct.schema.MetricSchema;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import scala.collection.JavaConversions;

public class MetricsFilterTest {

    private static SparkSession spark;

    private static LinkedList<String> data;

    @BeforeClass
    public static void setUpClass() throws Exception {
        spark = SparkSession.builder()
                        .appName("MetricSchemaTest")
                        .master("local[2]")
                        .getOrCreate();
        
        data = new LinkedList<>();
        data.add("{"
                    + "\"id\": \"id1\", "
                    + "\"host\": \"host1\", "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 12.45"
               + "}");
        data.add("{"
                    + "\"id\": \"id2\", "
                    + "\"host\": \"host2\", "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 1"
               + "}");
        data.add("{"
                    + "\"id\": \"id3\", "
                    + "\"host\": \"host2\", "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 1"
               + "}");
    }

    private StreamingQuery query;
    
    @Test
    public void expression() throws ConfigurationException {
        Properties props = new Properties();
        props.setProperty("expr", "att.id = 'id2'");
        
        testFilter(props, 1);
    }
    
    @Test
    public void attributesOneValue() throws ConfigurationException {
        Properties props = new Properties();
        props.setProperty("attribute.id", "id2");
        props.setProperty("attribute.host", "host2");
        
        testFilter(props, 1);
    }
    
    @Test
    public void attributesNegateOneValue() throws ConfigurationException {
        Properties props = new Properties();
        props.setProperty("attribute.host", "!host2");
        
        testFilter(props, 1);
    }
    
    @Test
    public void attributesMultiValue() throws ConfigurationException {
        Properties props = new Properties();
        props.setProperty("attribute.id", "'id1' 'id3'");
        
        testFilter(props, 2);
    }
    
    @Test
    public void attributesNegateMultiValue() throws ConfigurationException {        
        Properties props = new Properties();
        props.setProperty("attribute.id", "!'id1' 'id3'");
        
        testFilter(props, 1);
    }
    
    @Test
    public void attributesRlike() throws ConfigurationException {        
        Properties props = new Properties();
        props.setProperty("attribute.id", "rlike:^id1");
        
        testFilter(props, 1);
    }
    
    @Test
    public void attributesNotRlike() throws ConfigurationException {        
        Properties props = new Properties();
        props.setProperty("attribute.id", "!rlike:^id2");
        
        testFilter(props, 2);
    }
    
    public void testFilter(Properties filterProps, int expected) throws ConfigurationException {        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("timestamp.key", "time");
        props.setProperty("attributes.id", "id");
        props.setProperty("attributes.host", "host");
        props.setProperty("values.num_value.key", "num_value");
        for (Map.Entry<Object, Object> entry : filterProps.entrySet())
            props.setProperty("filter." + (String)entry.getKey(), (String)entry.getValue());
        
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        query.processAllAvailable();
        List<Row> results = spark.sql("select * from Output").collectAsList();
        assertEquals(expected, results.size());
    }
    
    @After
    public void clean() {
        query.stop();
    }
    
}
