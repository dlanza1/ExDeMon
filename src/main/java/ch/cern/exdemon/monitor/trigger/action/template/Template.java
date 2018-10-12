package ch.cern.exdemon.monitor.trigger.action.template;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.esotericsoftware.minlog.Log;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;
import ch.cern.utils.StringUtils;
import ch.cern.utils.TimeUtils;
import lombok.NonNull;

public class Template {
    
    public static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    
    public static synchronized String apply(String templateAsString, @NonNull Action action) {
        if(templateAsString == null)
            return null;
        
        TemplateString template = new TemplateString(templateAsString);
        
        template.replace("monitor_id", action.getMonitor_id());
        template.replace("trigger_id", action.getTrigger_id());
        
        //TODO DEPRECATED
        template.replaceContainer("agg_metrics", new SourceMetricsSupplier(action.getTriggeringResult()));
        //TODO DEPRECATED
        template.replaceContainer("source_metrics", new SourceMetricsSupplier(action.getTriggeringResult()));
        
        template.replaceKeys("attribute_value", action.getMetric_attributes());
        template.replaceKeys("attributes", new AttributesSupplier(action.getMetric_attributes()));
        
        template.replace("reason", action.getReason());
        
        Map<String, String> tags = action.getTags() != null ? action.getTags() : new HashMap<>();
         
        String tags_attributes = "";
        for(Map.Entry<String, String> tag: tags.entrySet())
            tags_attributes += "\n" + tag.getKey() + " = " + tag.getValue();
        if(tags.size() != 0)
            template.replace("tags", tags_attributes);
        else
            template.replace("tags", "(empty)");
        
        template.replaceKeys("tags", new TagsSupplier(action));
        
        AnalysisResult triggeringResult = action.getTriggeringResult();
        template.replace("triggering_value", triggeringResult.getAnalyzed_metric().getValue());
        template.replace("analysis_status", triggeringResult.getStatus().toString().toLowerCase());
        template.replaceKeys("analysis_param", triggeringResult.getAnalysisParams());
        template.replace("datetime", dateFormatter.format(action.getCreation_timestamp()));
        template.replaceKeys("datetime", new DateSupplier(action));
        
        return template.toString();
    }

    private static class AttributesSupplier implements ValueSupplier {

        private Map<String, String> attributes;

        public AttributesSupplier(Map<String, String> attributes) {
            this.attributes = attributes;
        }

        @Override
        public Object get(String keyPatternAndSeparatorsAsString) {
            String[] fields = keyPatternAndSeparatorsAsString.split("(?<!\\\\):");
            fields = Arrays.stream(fields)
                                .map(field -> field.replace("\\:", ":"))
                                .collect(Collectors.toList())
                                .toArray(new String[0]);
            
            Pattern keyPattern = Pattern.compile(fields[0]);
            
            String pairSeparator = "\n";
            if(fields.length > 1)
                pairSeparator = fields[1];
            
            String keyValueSeparator = " = ";
            if(fields.length > 2)
                keyValueSeparator = fields[2];
            
            List<Entry<String, String>> matchingAttributes = attributes.entrySet().stream()
                                                                        .filter(entry -> keyPattern.matcher(entry.getKey()).matches())
                                                                        .collect(Collectors.toList());
            
            if(!matchingAttributes.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                
                for(Map.Entry<String, String> att: matchingAttributes) {
                    String key = applyPatternGroup(keyPattern, att.getKey());
                    
                    sb.append(key + keyValueSeparator + StringUtils.removeTrailingZerosIfNumber(att.getValue()) + pairSeparator);
                }
                
                sb.delete(sb.length() - pairSeparator.length(), sb.length());
                
                return sb.toString();
            }else {
                return "";
            }
        }

        private String applyPatternGroup(Pattern keyPattern, String key) {
            Matcher matcher = keyPattern.matcher(key);
            
            if(matcher.find() && matcher.groupCount() > 0)
                return matcher.group(1);
            
            return key;
        }
        
    }
    
    private static class TagsSupplier implements ValueSupplier {

        private Action action;

        public TagsSupplier(Action action) {
            this.action = action;
        }

        @Override
        public Object get(String key) {
            Object value = action.getTags() != null ? action.getTags().get(key) : null;
            
            return apply(String.valueOf(value), action);
        }
        
    }
    
    private static class DateSupplier implements ValueSupplier {

        private Action action;

        public DateSupplier(Action action) {
            this.action = action;
        }

        @Override
        public Object get(String paramsAsString) {
            String[] params = paramsAsString.split(":");
            String format = params[0];
            
            Instant time = action.getCreation_timestamp();
            if(params.length > 1) {
                try {
                    Duration period = TimeUtils.parsePeriod(params[1]);
                    
                    time = time.plus(period);
                } catch (Exception e) {
                    Log.error("When parsing period for <datetime:" + paramsAsString + ">", e);
                }
            }
            
            String value = null;
            
            if(format.equals("utc"))
                value = String.valueOf(time);
            if(format.equals("ms"))
                value = String.valueOf(time.toEpochMilli());
            else
                value = String.valueOf(time);
            
            return value;
        }
        
    }
    
    private static class SourceMetricsSupplier implements ValueSupplier {

        private AnalysisResult triggeringResult;

        public SourceMetricsSupplier(AnalysisResult triggeringResult) {
            this.triggeringResult = triggeringResult;
        }

        @Override
        public Object get(String metricTemplateAsString) {
            TemplateString globalMetricTemplate = new TemplateString(metricTemplateAsString);
            
            List<Metric> lastSourceMetrics = triggeringResult.getAnalyzed_metric().getValue().getLastSourceMetrics();
            if(lastSourceMetrics == null)
                return "No source metrics.";
            
            MetricsFilter metricsFilter = getMetricsFilter(globalMetricTemplate);
            List<Metric> metrics = lastSourceMetrics.stream()
                                                        .filter(metricsFilter::test)
                                                        .sorted((a, b) -> -1 * a.getTimestamp().compareTo(b.getTimestamp()))
                                                        .collect(Collectors.toList());
            
            if(metrics.isEmpty())
                return "No source metrics.";
                
            String finalText = "";
            for (Metric metric : metrics) {
                TemplateString metricTemplate = globalMetricTemplate.clone();
                
                metricTemplate.replaceKeys("attribute_value", metric.getAttributes());
                metricTemplate.replaceKeys("attributes", new AttributesSupplier(metric.getAttributes()));
                metricTemplate.replace("datetime", dateFormatter.format(metric.getTimestamp()));
                metricTemplate.replace("value", String.valueOf(metric.getValue()));
                
                finalText = finalText.concat(metricTemplate.toString());
            }
            
            return finalText;
        }

        private MetricsFilter getMetricsFilter(TemplateString template) {
            MetricsFilter filter = new MetricsFilter();
            
            Properties props = new Properties();
           
            Matcher filter_exprMatcher = Pattern.compile("\\<filter_expr:([^>]+)\\>").matcher(template.toString());     
            if(filter_exprMatcher.find()) {
                String filterExpression = filter_exprMatcher.group(1);
                props.setProperty("expr", filterExpression);
                
                template.replace("filter_expr:"+filterExpression, "");
            }
            
            filter.config(props);
            
            return filter;
        }
        
    }
    
}
