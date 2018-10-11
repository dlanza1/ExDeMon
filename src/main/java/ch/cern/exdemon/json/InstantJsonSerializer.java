package ch.cern.exdemon.json;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class InstantJsonSerializer implements JsonSerializer<Instant> {

    public static DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneOffset.systemDefault());
    
    private DateTimeFormatter formatter;

    public InstantJsonSerializer() {
        this.formatter = DEFAULT_FORMATTER;
    }
    
    public InstantJsonSerializer(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }
    
    @Override
    public JsonElement serialize(Instant instant, java.lang.reflect.Type type, JsonSerializationContext context) {
        return new JsonPrimitive(ZonedDateTime.ofInstant(instant, ZoneOffset.systemDefault()).format(formatter));
    }
    
}
