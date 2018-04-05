package ch.cern.exdemon.metric.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.cern.exdemon.metric.Metric;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import scala.collection.JavaConversions;

public class MetricSchemaTest {
    
    private static SparkSession spark;

    @BeforeClass
    public static void setUpClass() throws Exception {
        spark = SparkSession.builder()
                        .appName("MetricSchemaTest")
                        .master("local[2]")
                        .getOrCreate();
    }
    
    @Test
    public void parse() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"id\": \"id1\", "
                    + "\"tag\": { \"tag1\": \"tagValue11\", \"tag2\": \"tagValue12\" }, "
                    + "\"time\": 1522919277, "
                    + "\"num_value\": 12.45,"
                    + "\"bool_value\": false,"
                    + "\"str_value\": \"sting_value\""
               + "}");
        data.add("{"
                    + "\"id\": \"id2\", "
                    + "\"tag\": { \"tag1\": \"tagValue21\", \"tag2\": \"tagValue22\" }, "
                    + "\"time\": 1522009277, "
                    + "\"num_value\": 9,"
                    + "\"bool_value\": true,"
                    + "\"str_value\": \"123456\""
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("timestamp.key", "time");
        props.setProperty("timestamp.format", "epoch-s");
        props.setProperty("attributes.id", "id");
        props.setProperty("attributes.tag1", "tag.tag1");
        props.setProperty("attributes.tag2", "tag.tag2");
        props.setProperty("values.num_value.key", "num_value");
        props.setProperty("values.num_value.type", "numeric");
        props.setProperty("values.bool_value.key", "bool_value");
        props.setProperty("values.bool_value.type", "boolean");
        props.setProperty("values.str_value.key", "str_value");
        props.setProperty("values.str_value.type", "string");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap);
        
        StreamingQuery query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> jsonList = spark.sql("select * from Output").collectAsList();
        for (Row row : jsonList) {
            System.out.println(row);
        }
        
        query.stop();
    }
    
    @Test
    public void parseAttributes() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"id\": \"id1\", "
                    + "\"tag\": { \"tag1\": \"tagValue11\", \"tag2\": \"tagValue12\" }, "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 12.45"
               + "}");
        
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
        props.setProperty("attributes.tag1", "tag.tag1");
        props.setProperty("attributes.tag2", "tag.tag2");
        props.setProperty("attributes.tag3", "tag.tag3");
        props.setProperty("values.num_value.key", "num_value");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap);
        
        StreamingQuery query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        Row attStruct = row.getStruct(row.fieldIndex("att"));
        
        assertEquals("test-source", attStruct.getString(attStruct.fieldIndex("$source")));
        assertEquals("test", attStruct.getString(attStruct.fieldIndex("$schema")));
        assertEquals("num_value", attStruct.getString(attStruct.fieldIndex("$value")));
        assertEquals("tagValue11", attStruct.getString(attStruct.fieldIndex("tag1")));
        assertEquals("tagValue12", attStruct.getString(attStruct.fieldIndex("tag2")));
        assertNull(attStruct.getString(attStruct.fieldIndex("tag3")));
        
        query.stop();
    }
    
    @Test
    public void timestampEpochS() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"time\": 1522919277, "
                    + "\"num_value\": 12.45"
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("timestamp.key", "time");
        props.setProperty("timestamp.format", "epoch-s");
        props.setProperty("values.num_value.key", "num_value");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap);
        
        StreamingQuery query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        assertEquals(new Timestamp(1522919277000l), row.getTimestamp(row.fieldIndex("timestamp")));
        
        query.stop();
    }
    
    @Test
    public void timestampEpochMs() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 12.45"
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("timestamp.key", "time");
        props.setProperty("timestamp.format", "epoch-ms");
        props.setProperty("values.num_value.key", "num_value");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap);
        
        StreamingQuery query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        assertEquals(new Timestamp(1522919277000l), row.getTimestamp(row.fieldIndex("timestamp")));
        
        query.stop();
    }
    
}
