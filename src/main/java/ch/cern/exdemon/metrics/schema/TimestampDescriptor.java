package ch.cern.exdemon.metrics.schema;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.DurationAndTruncate;
import lombok.ToString;

@ToString
public class TimestampDescriptor {
    
    private static final Pattern INTEGER_NUMBER = Pattern.compile("\\d+");

    public static final String KEY_PARAM = "key";
    protected String key;
    
    public static final String REGEX_PARAM = "regex";
    private Pattern regex;
    
    public static final String FORMAT_PARAM = "format";
    public static final String FORMAT_DEFAULT = "auto";
    public transient DateTimeFormatter format_auto;
    protected String format_pattern;
    protected transient DateTimeFormatter timestamp_format;
    
    public static final String SHIFT_PARAM = "shift";
    protected DurationAndTruncate shift;

    public TimestampDescriptor() {
    }
    
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        key = properties.getProperty(KEY_PARAM);
        
        String regexString = properties.getProperty(REGEX_PARAM);
        if(regexString != null) {
            regex = Pattern.compile(regexString);
            
            if(regex.matcher("").groupCount() != 1)
                confResult.withError(REGEX_PARAM, "regex expression must contain exactly 1 capture group from which timestamp will be extracted");
        }else{
            regex = null;
        }
        
        format_pattern = properties.getProperty(FORMAT_PARAM, FORMAT_DEFAULT);
        if (!format_pattern.equals("epoch-ms")
                && !format_pattern.equals("epoch-s")
                && !format_pattern.equals("auto")) {
            try {
                new DateTimeFormatterBuilder().appendPattern(format_pattern).toFormatter()
                        .withZone(ZoneOffset.systemDefault());
            } catch (Exception e) {
                confResult.withError(MetricSchema.TIMESTAMP_PARAM + "." + FORMAT_PARAM,
                                "must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.");
            }
        }
        
        String shiftString = properties.getProperty(SHIFT_PARAM);
        if(shiftString != null)
            try {
                shift = DurationAndTruncate.from(shiftString);
            } catch (ConfigurationException e) {
                confResult.withError(SHIFT_PARAM, e);
            }
        else
            shift = new DurationAndTruncate(Duration.ZERO);
        
        return confResult;
    }

    public Instant extract(JSON jsonObject) throws Exception {
        if(key != null) {
            String timestamp_string = jsonObject.getProperty(key);
            
            if (timestamp_string == null || timestamp_string.length() == 0)
                throw new DateTimeParseException("No data to parse", "", 0);
            
            if(regex != null) {
                Matcher matcher = regex.matcher(timestamp_string);
                
                if(matcher.find()) {
                    timestamp_string = matcher.group(1);
                }else{
                    throw new DateTimeParseException("regex (" + regex + ") did not matched for value (" + timestamp_string + ")", timestamp_string, 0);
                }
            }
            
            try {
                return shift.adjustPlus(toDate(timestamp_string));
            } catch (Exception e) {
                throw new DateTimeParseException(e.getMessage() + " for key " + key + " with value (" + timestamp_string + ")", timestamp_string, 0);
            }
        }else {
            return shift.adjustPlus(Instant.now());
        }
    }

    private Instant toDate(String date_string) throws DateTimeParseException {
        if(format_auto == null)
            format_auto = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd['T'][ ]HH:mm:ss[.SSS][Z]").toFormatter();

        try {
            if (format_pattern.equals("auto")) {
                try {
                    if(INTEGER_NUMBER.matcher(date_string).matches()) {
                        long value = Long.valueOf(date_string);
                        
                        if(value < Math.pow(10, 10))
                            return Instant.ofEpochSecond(value);
                        else
                            return Instant.ofEpochMilli(value);
                    }
                }catch(Exception e) {}
                
                try {
                    if(date_string.contains("T") && date_string.endsWith("Z"))
                        return Instant.parse(date_string);
                }catch(Exception e) {}

                try {
                    TemporalAccessor temporalAccesor = format_auto.parse(date_string);
                    
                    if (temporalAccesor.isSupported(ChronoField.INSTANT_SECONDS))
                        return Instant.from(temporalAccesor);
                    else
                        return LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
                }catch(Exception e) {}
                
                throw new DateTimeParseException("Automatic format could not parse time", "", 0);
            }
                
            if (format_pattern.equals("epoch-ms"))
                return Instant.ofEpochMilli(Long.valueOf(date_string));

            if (format_pattern.equals("epoch-s"))
                return Instant.ofEpochSecond(Long.valueOf(date_string));
        } catch (Exception e) {
            throw new DateTimeParseException(e.getClass().getName() + ": " + e.getMessage(), date_string, 0);
        }

        if (timestamp_format == null)
            timestamp_format = new DateTimeFormatterBuilder()
                                            .appendPattern(format_pattern)
                                            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .toFormatter();
        
        TemporalAccessor temporalAccesor = timestamp_format.parse(date_string);
        
        if (temporalAccesor.isSupported(ChronoField.INSTANT_SECONDS))
            return Instant.from(temporalAccesor);
        else
            return LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
    }

}
