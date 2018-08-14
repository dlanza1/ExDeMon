package ch.cern.utils;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

import ch.cern.properties.ConfigurationException;

public class TimeUtils {

    public static DateTimeFormatter TIMESTAMP_FORMAT_DEFAULT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneOffset.systemDefault());

    public static Instant toInstant(String date_string) throws ParseException {
        return TIMESTAMP_FORMAT_DEFAULT.parse(date_string, Instant::from);
    }

    public static LocalDateTime toLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
    }

    public static Duration parsePeriod(String input) throws ConfigurationException {
        Objects.requireNonNull(input);

        Optional<Character> unit = StringUtils.getLastCharacter(input).filter(Character::isLetter);

        if (unit.isPresent()) {
            String number_string = input.substring(0, input.length() - 1);
            long number = Long.parseLong(number_string);

            switch (unit.get()) {
            case 'w':
                return Duration.ofDays(number * 7);
            case 'd':
                return Duration.ofDays(number);
            case 'h':
                return Duration.ofHours(number);
            case 'm':
                return Duration.ofMinutes(number);
            case 's':
                return Duration.ofSeconds(number);
            default:
                throw new ConfigurationException(null, "unit " + unit.get() + " not available.");
            }
        }

        return Duration.ofSeconds(Long.parseLong(input));
    }

    public static String toString(Duration duration) {
        long seconds = duration.getSeconds();

        if (seconds == 0L || duration == Duration.ZERO)
            return "0 seconds";

        int hours = (int) (seconds / 3600);
        seconds -= 3600 * hours;
        int minutes = (int) (seconds / 60);
        seconds -= 60 * minutes;

        String hours_s = null;
        if (hours > 1)
            hours_s = hours + " hours";
        else if (hours == 1)
            hours_s = "1 hour";

        String minutes_s = null;
        if (minutes > 1)
            minutes_s = minutes + " minutes";
        else if (minutes == 1)
            minutes_s = "1 minute";

        String seconds_s = null;
        if (seconds > 1)
            seconds_s = seconds + " seconds";
        else if (seconds == 1)
            seconds_s = "1 second";

        if (hours_s != null && minutes_s == null && seconds_s == null)
            return hours_s;
        if (hours_s == null && minutes_s != null && seconds_s == null)
            return minutes_s;
        if (hours_s == null && minutes_s == null && seconds_s != null)
            return seconds_s;

        if (hours_s != null && minutes_s != null && seconds_s == null)
            return hours_s + " and " + minutes_s;
        if (hours_s == null && minutes_s != null && seconds_s != null)
            return minutes_s + " and " + seconds_s;
        if (hours_s != null && minutes_s == null && seconds_s != null)
            return hours_s + " and " + seconds_s;

        return hours_s + ", " + minutes_s + " and " + seconds_s;
    }

    public static ChronoUnit parseGranularity(String granularityString) throws ConfigurationException {
        switch (granularityString) {
        case "d":
            return ChronoUnit.DAYS;
        case "h":
            return ChronoUnit.HOURS;
        case "m":
            return ChronoUnit.MINUTES;
        case "s":
            return ChronoUnit.SECONDS;
        case "ms":
            return ChronoUnit.MILLIS;
        default:
            throw new ConfigurationException(null, "granularity is configured to an incompatible value.");
        }
    }

}
