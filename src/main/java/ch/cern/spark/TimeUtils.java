package ch.cern.spark;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class TimeUtils {
	
    public static DateTimeFormatter TIMESTAMP_FORMAT_DEFAULT = new DateTimeFormatterBuilder()
			.appendPattern("yyyy-MM-dd HH:mm:ss")
			.toFormatter()
			.withZone(ZoneOffset.systemDefault());
    
    public static Instant toInstant(String date_string) throws ParseException {
        return TIMESTAMP_FORMAT_DEFAULT.parse(date_string, Instant::from);
    }

    public static Instant toInstant(int seconds) {
		return Instant.ofEpochMilli(seconds * 1000);
	}

	public static LocalDateTime toLocalDateTime(Instant instant) {
		return LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
	}
    
}
