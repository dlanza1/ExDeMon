package ch.cern.exdemon.metrics.schema;

import static ch.cern.exdemon.metrics.schema.TimestampDescriptor.FORMAT_PARAM;
import static ch.cern.exdemon.metrics.schema.TimestampDescriptor.KEY_PARAM;
import static ch.cern.exdemon.metrics.schema.TimestampDescriptor.REGEX_PARAM;
import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

import org.junit.Test;

import com.google.gson.JsonObject;

import ch.cern.exdemon.json.JSON;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class TimestampDescriptorTest {
    
    @Test
    public void shift() throws Exception {
        TimestampDescriptor descriptor = new TimestampDescriptor();
        Properties properties = new Properties();
        properties.setProperty(KEY_PARAM, "time");
        properties.setProperty("shift", "-3h");
        descriptor.config(properties);

        Instant time = Instant.now();
        
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", time.toString());
        Instant result = descriptor.extract(new JSON(jsonObject));
        
        Instant expectedResult = time.minus(Duration.ofHours(3));
        
        assertEquals(expectedResult, result);
    }
    

    @Test
    public void currentTimeIfKeyNotConfigured() throws Exception {
        Properties props = new Properties();
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JSON jsonObject = new JSON("{}");
        Instant result = descriptor.extract(jsonObject);
        assertEquals(Instant.now().toEpochMilli(), result.toEpochMilli(), 10);
    }
    
    @Test
    public void regex() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(REGEX_PARAM, "aa(\\d+)bb");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "aa1234bb");
        Instant result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1234000, result.toEpochMilli());
    }
    
    @Test
    public void regexWrongConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(REGEX_PARAM, "aa\\d+bb");
        
        TimestampDescriptor descriptor = new TimestampDescriptor();
        
        assertEquals("regex expression must contain exactly 1 capture group from which timestamp will be extracted", 
                      descriptor.config(props).getErrors().get(0).getMessage());
    }

    @Test
    public void formatAuto() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "auto");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", 1509520209883l);
        Instant result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209883l, result.toEpochMilli());

        jsonObject = new JsonObject();
        jsonObject.addProperty("time", 1509520209);
        result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209000l, result.toEpochMilli());

        jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2017-11-01T08:10:09+0100");
        result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209000l, result.toEpochMilli());

        jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2017-11-01 08:10:09+0100");
        result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209000l, result.toEpochMilli());
        
        jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2018-02-12T11:51:13.963Z");
        result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1518436273963l, result.toEpochMilli());
    }

    @Test
    public void formatMs() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "epoch-ms");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", 1509520209883l);
        Instant result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209883l, result.toEpochMilli());
    }

    @Test
    public void formatS() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "epoch-s");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", 1509520209);
        Instant result = descriptor.extract(new JSON(jsonObject));
        assertEquals(1509520209000l, result.toEpochMilli());
    }

    @Test
    public void formatPattern() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "yyyy-MM-dd HH:mm:ssZ");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2017-10-20 02:00:12+0000");
        Instant result = descriptor.extract(new JSON(jsonObject));
        assertEquals(Instant.parse("2017-10-20T02:00:12.000Z").toEpochMilli(), result.toEpochMilli());
    }

    @Test
    public void formatPatternWithoutTimeZone()
            throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "yyyy-MM-dd HH:mm:ss");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2017-12-20 02:00:12");
        Instant result = descriptor.extract(new JSON(jsonObject));

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                                    .appendPattern(props.getProperty(FORMAT_PARAM))
                                                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                                    .toFormatter();

        // Value should be --> Fri Oct 20 2017 00:00:12 UTC
        TemporalAccessor temporalAccesor = formatter.parse("2017-12-20 02:00:12");
        Instant timestamp = LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
        
        assertEquals(timestamp, result);
    }

    @Test
    public void formatPatternWithoutHour() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "yyyy MM dd");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "2017 12 20");
        Instant result = descriptor.extract(new JSON(jsonObject));

        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy MM dd HH:mm:ss").toFormatter()
                .withZone(ZoneId.systemDefault());

        TemporalAccessor temporalAccesor = formatter.parse("2017 12 20 00:00:00");
        Instant timestamp = LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
        assertEquals(timestamp, result);
    }

    @Test(expected=DateTimeParseException.class)
    public void exceptionWhenPatternIfUnexpectedFormat()
            throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "yyyy MM dd");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "11:34");
        descriptor.extract(new JSON(jsonObject));
    }

    @Test(expected=DateTimeParseException.class)
    public void exceptionWhenEpochIfUnexpectedFormat()
            throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "epoch-ms");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        descriptor.config(props);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", "10:23");
        descriptor.extract(new JSON(jsonObject));
    }

    @Test
    public void foramtWrongConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(KEY_PARAM, "time");
        props.setProperty(FORMAT_PARAM, "wrong_format");
        TimestampDescriptor descriptor = new TimestampDescriptor();
        
        assertEquals("must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.", 
                descriptor.config(props).getErrors().get(0).getMessage());
    }
    
}
