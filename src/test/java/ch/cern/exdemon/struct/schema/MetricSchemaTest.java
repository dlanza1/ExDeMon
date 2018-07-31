package ch.cern.exdemon.struct.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Timestamp;
import java.time.Instant;
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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.cern.exdemon.struct.Metric;
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

    private StreamingQuery query;
    
    @Test
    public void parseValues() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"num_value\": 9.98,"
                    + "\"bool_value\": true,"
                    + "\"str_value\": { \"level1\": \"string_string\"}"
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("values.n_value.key", "num_value");
        props.setProperty("values.b_value.key", "bool_value");
        props.setProperty("values.s_value.key", "str_value.level1");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        
        Row row = results.get(0);
        Row rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals("string_string", rowValue.getString(rowValue.fieldIndex("str")));

        row = results.get(1);
        rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals(9.98, rowValue.getDouble(rowValue.fieldIndex("num")), 0);
        assertEquals("9.98", rowValue.getString(rowValue.fieldIndex("str")));
        
        row = results.get(2);
        rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals(true, rowValue.getBoolean(rowValue.fieldIndex("bool")));
        assertEquals("true", rowValue.getString(rowValue.fieldIndex("str")));
    }
    
    @Test
    public void parseTypedValues() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"num_value\": 9.98,"
                    + "\"bool_value\": true,"
                    + "\"str_value\": \"string_value\""
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("values.num_value.key", "num_value");
        props.setProperty("values.num_value.type", "numeric");
        props.setProperty("values.bool_value.key", "bool_value");
        props.setProperty("values.bool_value.type", "boolean");
        props.setProperty("values.str_value.key", "str_value");
        props.setProperty("values.str_value.type", "string");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        
        Row row = results.get(0);
        Row rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals("string_value", rowValue.getString(rowValue.fieldIndex("str")));
        
        row = results.get(1);
        rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals(9.98, rowValue.getDouble(rowValue.fieldIndex("num")), 0);
        
        row = results.get(2);
        rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals(true, rowValue.getBoolean(rowValue.fieldIndex("bool")));
    }
    
    @Test
    public void regexValues() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"str_value\": \"v1=123.12 v2=home\""
               + "}");
        
        MemoryStream<String> input = new MemoryStream<String>(10, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(data));
        
        Dataset<String> json = input.toDF().as(Encoders.STRING());
        
        HashMap<String, Dataset<String>> allSourcesMap = new HashMap<>();
        allSourcesMap.put("test-source", json);
        
        MetricSchema schema = new MetricSchema("test");
        Properties props = new Properties();
        props.setProperty("sources", "test-source");
        props.setProperty("values.v1.key", "str_value");
        props.setProperty("values.v1.regex", ".*v1=([0-9.]+).*");
        props.setProperty("values.v2.key", "str_value");
        props.setProperty("values.v2.regex", ".*v2=(\\w+).*");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        
        Row row = results.get(0);

        Row rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals(123.12, rowValue.getDouble(rowValue.fieldIndex("num")), 0f);
        
        row = results.get(1);
        rowValue = row.getStruct(row.fieldIndex("value"));
        assertEquals("home", rowValue.getString(rowValue.fieldIndex("str")));
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
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
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
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        assertEquals(new Timestamp(1522919277000l), row.getTimestamp(row.fieldIndex("timestamp")));
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
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        assertEquals(new Timestamp(1522919277000l), row.getTimestamp(row.fieldIndex("timestamp")));
    }
    
    @Test
    public void timestampPattern() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"time\": \"2017-08-01 02:26:59\", "
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
        props.setProperty("timestamp.format", "yyyy-MM-dd HH:mm:ss");
        props.setProperty("values.num_value.key", "num_value");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        
        query.processAllAvailable();
        
        List<Row> results = spark.sql("select * from Output").collectAsList();
        Row row = results.get(0);
        assertEquals(Instant.parse("2017-08-01T00:26:59.000Z"), row.getTimestamp(row.fieldIndex("timestamp")).toInstant());
    }
    
    @Test
    public void filter() throws ConfigurationException {
        List<String> data = new LinkedList<>();
        data.add("{"
                    + "\"id\": \"id1\", "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 12.45"
               + "}");
        data.add("{"
                    + "\"id\": \"id2\", "
                    + "\"time\": 1522919277000, "
                    + "\"num_value\": 1"
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
        props.setProperty("filter.expr", "att.id = 'id1'");
        schema.config(props);
        
        Dataset<Metric> metrics = schema.apply(allSourcesMap).get();
        query = metrics.writeStream()
                    .format("memory")
                    .queryName("Output")
                    .outputMode(OutputMode.Append())
                    .start();
        query.processAllAvailable();
        List<Row> results = spark.sql("select * from Output").collectAsList();
        assertEquals(1, results.size());
    }
    
    @After
    public void clean() {
        query.stop();
    }
    
}
