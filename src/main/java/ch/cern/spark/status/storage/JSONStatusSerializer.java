package ch.cern.spark.status.storage;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.numenta.nupic.algorithms.AnomalyLikelihood;
import org.numenta.nupic.network.Network;
import org.reflections.Reflections;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import ch.cern.exdemon.metrics.defined.equation.var.VariableStatus;
import ch.cern.exdemon.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.CountAgregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.CountFloatsAgregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.LastValueAggregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.SuccessiveCountAgregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.SumAggregation;
import ch.cern.exdemon.metrics.value.AggregatedValue;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.exdemon.monitor.analysis.types.HTMAnalysis;
import ch.cern.exdemon.monitor.trigger.TriggerStatus;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;
import ch.cern.exdemon.monitor.trigger.types.ConstantTrigger;
import ch.cern.exdemon.monitor.trigger.types.PercentageTrigger;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public class JSONStatusSerializer implements StatusSerializer {

    private static final long serialVersionUID = 3041033624802735579L;

    private static Gson parser = new GsonBuilder().registerTypeAdapter(Instant.class, new InstantAdapter())
                                                    .registerTypeAdapter(Value.class, new ValueAdapter())
                                                    .registerTypeAdapter(Aggregation.class, new HierarchyAdapter<Aggregation>())
                                                    .registerTypeAdapter(VariableStatus.class, new HierarchyAdapter<Aggregation>())
                                                    .registerTypeAdapter(StatusKey.class, new HierarchyAdapter<StatusKey>())
                                                    .registerTypeAdapter(StatusValue.class, new HierarchyAdapter<StatusValue>())
                                                    .registerTypeAdapter(Network.class, new HTMAnalysis.JsonAdapter())
                                                    .registerTypeAdapter(AnomalyLikelihood.class, new HTMAnalysis.AnomalyLikelihoodJsonAdapter())
                                                    .create();
    
    @Override
    public byte[] fromKey(StatusKey key) throws IOException {
        return parser.toJson(key, StatusKey.class).getBytes();
    }
    @Override
    public StatusKey toKey(byte[] bytes) throws IOException {
        return parser.fromJson(new String(bytes), StatusKey.class);
    }

    @Override
    public byte[] fromValue(StatusValue value) throws IOException {
        return parser.toJson(value, StatusValue.class).getBytes();
    }

    @Override
    public StatusValue toValue(byte[] bytes) throws IOException {
        return parser.fromJson(new String(bytes), StatusValue.class);
    }

    public static class InstantAdapter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {

        @Override
        public Instant deserialize(JsonElement json, Type type, JsonDeserializationContext context)
                throws JsonParseException {
            return Instant.ofEpochMilli(json.getAsLong());
        }

        @Override
        public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(instant.toEpochMilli());
        }

    }

    public static class ValueAdapter implements JsonSerializer<Value>, JsonDeserializer<Value> {

        @Override
        public Value deserialize(JsonElement json, Type type, JsonDeserializationContext context)
                throws JsonParseException {
            
            JsonObject jsonObject = json.getAsJsonObject();
            
            if(jsonObject.has("str"))
                return context.deserialize(json, StringValue.class);
            else if(jsonObject.has("num"))
                return context.deserialize(json, FloatValue.class);
            else if(jsonObject.has("bool"))
                return context.deserialize(json, BooleanValue.class);
            else if(jsonObject.has("exception_message"))
                return context.deserialize(json, ExceptionValue.class);
            else if(jsonObject.has("agg"))
                return context.deserialize(json, AggregatedValue.class);
            else if(jsonObject.has("properties_name"))
                return context.deserialize(json, PropertiesValue.class);
            else
                throw new JsonParseException("Type of Value not known");
        }

        @Override
        public JsonElement serialize(Value value, Type type, JsonSerializationContext context) {
            return context.serialize(value);
        }

    }

    public static class HierarchyAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {

        private static String KEY_TYPE = "fqcn";
        private static String KEY_ALIAS_TYPE = "fqcn-alias";

        private static Map<String, Class<?>> aliases = new HashMap<>();
        {
            new Reflections("ch.cern").getTypesAnnotatedWith(ClassNameAlias.class).stream().forEach(
                    type -> aliases.put(type.getAnnotation(ClassNameAlias.class).value(), type));
        }
        
        private static Map<String, Class<?>> nameChangedAliases = new HashMap<>();
        {
            nameChangedAliases.put("notificator-key", TriggerStatusKey.class);
            nameChangedAliases.put("notificator-status", TriggerStatus.class);
            nameChangedAliases.put("constant-notificator", ConstantTrigger.Status_.class);
            nameChangedAliases.put("percentage-trigger", PercentageTrigger.Status_.class);
        }
        
        private static Map<String, Class<?>> classNameChanged = new HashMap<>();
        {
            classNameChanged.put("ch.cern.spark.metrics.defined.equation.var.agg.LastValueAggregation", LastValueAggregation.class);
            classNameChanged.put("ch.cern.spark.metrics.defined.equation.var.agg.CountAgregation", CountAgregation.class);
            classNameChanged.put("ch.cern.spark.metrics.defined.equation.var.agg.SuccessiveCountAgregation", SuccessiveCountAgregation.class);
            classNameChanged.put("ch.cern.spark.metrics.defined.equation.var.agg.CountFloatsAgregation", CountFloatsAgregation.class);
            classNameChanged.put("ch.cern.spark.metrics.defined.equation.var.agg.SumAggregation", SumAggregation.class);
        }
        
        private static Set<String> deprecatedAliases = new HashSet<>();
        {
            deprecatedAliases.add("statuses-notificator");
            deprecatedAliases.add("metric-variable");
        }

        @Override
        public T deserialize(JsonElement json, Type type, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();

            Class<?> klass = null;

            JsonElement aliasElement = jsonObject.get(KEY_ALIAS_TYPE);
            if (aliasElement != null) {
                if (!aliases.containsKey(aliasElement.getAsString()) && !nameChangedAliases.containsKey(aliasElement.getAsString())) {
                    if(deprecatedAliases.contains(aliasElement.getAsString()))
                        return null;
                    else
                        throw new JsonParseException("Document contains an alias ("+aliasElement+") that is not registered. Registered: " + aliases);
                }

                klass = aliases.get(aliasElement.getAsString());
                if(klass == null)
                    klass = nameChangedAliases.get(aliasElement.getAsString());
                
                jsonObject.remove(KEY_ALIAS_TYPE);
            }

            if (klass == null) {
                JsonElement FQCNelement = jsonObject.get(KEY_TYPE);
                if (FQCNelement == null || !FQCNelement.isJsonPrimitive() || !((JsonPrimitive) FQCNelement).isString())
                    throw new JsonParseException(KEY_TYPE + " or " + KEY_ALIAS_TYPE + " is not contained in the document as string.");

                String fqcn = FQCNelement.getAsString();
                
                try {
                    klass = (Class<?>) Class.forName(fqcn);
                } catch (ClassNotFoundException e) {
                    if(classNameChanged.containsKey(fqcn))
                        klass = classNameChanged.get(fqcn);
                    else
                        throw new JsonParseException("No class for " + fqcn, e);
                }
                jsonObject.remove(KEY_TYPE);
            }

            return context.deserialize(jsonObject, klass);
        }

        @Override
        public JsonElement serialize(T object, Type type, JsonSerializationContext context) {
            JsonObject json = context.serialize(object).getAsJsonObject();
            
            Class<?> klass = object.getClass();
            if (klass.isAnnotationPresent(ClassNameAlias.class))
                json.addProperty(KEY_ALIAS_TYPE, klass.getAnnotation(ClassNameAlias.class).value());
            else
                json.addProperty(KEY_TYPE, object.getClass().getName());

            return json;
        }

    }

}
