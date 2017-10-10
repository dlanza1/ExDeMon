package ch.cern.spark;

import java.text.ParseException;
import java.time.Duration;
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

	public static LocalDateTime toLocalDateTime(Instant instant) {
		return LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
	}
	
    public static Duration parsePeriod(String input) {
        char unit = StringUtils.getLastCharacter(input);

        String number_string = input.substring(0, input.length() - 1);
        if(number_string.length() > 0){
            long number = Long.parseLong(number_string);
            
            switch (unit) {
            case 'h':
                return Duration.ofHours(number);
            case 'm':
                return Duration.ofMinutes(number);
            case 's':            
                return Duration.ofSeconds(number);
            }
        }

        return Duration.ofSeconds(Long.parseLong(input));
    }

    
}
