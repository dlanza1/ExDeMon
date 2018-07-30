package ch.cern.exdemon.monitor.trigger.action;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import lombok.NonNull;

public class Template {
    
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd hh:mm:ss").withZone(ZoneId.systemDefault());
    
    public static String apply(String template, @NonNull Action action) {
        if(template == null)
            return null;
        
        String text = template;
        
        text = text.replaceAll("<monitor_id>", String.valueOf(action.getMonitor_id()));
        
        text = text.replaceAll("<trigger_id>", String.valueOf(action.getTrigger_id()));
        
        Map<String, String> attributes = action.getMetric_attributes() != null ? action.getMetric_attributes() : new HashMap<>();
         
        String metric_attributes = "";
        for(Map.Entry<String, String> att: attributes.entrySet())
            metric_attributes += "\n\t" + att.getKey() + " = " + att.getValue();
        if(attributes.size() != 0)
            text = text.replaceAll("<metric_attributes>", metric_attributes);
        else
            text = text.replaceAll("<metric_attributes>", "(no attributes)");
        
        Matcher attMatcher = Pattern.compile("\\<metric_attributes:([^>]+)\\>").matcher(text);        
        while (attMatcher.find()) {
            for (int j = 1; j <= attMatcher.groupCount(); j++) {
                String key = attMatcher.group(j);
                
                String value = attributes.get(key);
                
                text = text.replaceAll("<metric_attributes:"+key+">", String.valueOf(value));
                
                j++;
            }
        }
        
        text = text.replaceAll("<reason>", String.valueOf(action.getReason()));
        
        Map<String, String> tags = action.getTags() != null ? action.getTags() : new HashMap<>();
         
        String tags_attributes = "";
        for(Map.Entry<String, String> tag: tags.entrySet())
            tags_attributes += "\n\t" + tag.getKey() + " = " + tag.getValue();
        if(tags.size() != 0)
            text = text.replaceAll("<tags>", tags_attributes);
        else
            text = text.replaceAll("<tags>", "(empty)");
        
        Matcher tagsMatcher = Pattern.compile("\\<tags:([^>]+)\\>").matcher(text);        
        while (tagsMatcher.find()) {
            for (int j = 1; j <= tagsMatcher.groupCount(); j++) {
                String key = tagsMatcher.group(j);
                
                String value = apply(tags.get(key), action);
                
                text = text.replaceAll("<tags:"+key+">", String.valueOf(value));
                
                j++;
            }
        }

        AnalysisResult triggeringResult = action.getTriggeringResult();
        
        text = text.replaceAll("<triggering_value>", String.valueOf(triggeringResult.getAnalyzed_metric().getValue()));
        
        text = aggregatedMetrics(text, triggeringResult);
        
        text = text.replaceAll("<analysis_status>", String.valueOf(triggeringResult.getStatus().toString().toLowerCase()));
        
        Map<String, Object> analysisParams = triggeringResult.getAnalysisParams();
        Matcher analysisParamMatcher = Pattern.compile("\\<analysis_param:([^>]+)\\>").matcher(text);        
        while (analysisParamMatcher.find()) {
            for (int j = 1; j <= analysisParamMatcher.groupCount(); j++) {
                String key = analysisParamMatcher.group(j);
                
                String value = String.valueOf(analysisParams.get(key));
                
                text = text.replaceAll("<analysis_param:"+key+">", value);
                
                j++;
            }
        }
        
        text = text.replaceAll("<datetime>", dateFormatter.format(action.getCreation_timestamp()));
        
        return text;
    }

    private static String aggregatedMetrics(String template, AnalysisResult triggeringResult) {
        int startAggMetrics = template.indexOf("<agg_metrics>");
        
        while (startAggMetrics > -1){
        	int endAggMetrics = template.indexOf("</agg_metrics>", startAggMetrics);
        	
        	String preText = template.substring(0, startAggMetrics);
        	String metricTemplate = template.substring(startAggMetrics + 13, endAggMetrics);
        	String postText = template.substring(endAggMetrics + 14);
        	
        	List<Metric> lastSourceMetrics = triggeringResult.getAnalyzed_metric().getValue().getLastSourceMetrics();
        	if(lastSourceMetrics == null || lastSourceMetrics.isEmpty()) {
        		template = preText + "No aggregated metrics." + postText;
        		break;
        	}
        		
        	String metricsText = "";
        	for (Metric metric : lastSourceMetrics) {
        		String metricText = metricTemplate;
        		
        		Matcher attributeMatcher = Pattern.compile("\\<attribute:([^>]+)\\>").matcher(metricText);        
                while (attributeMatcher.find()) {
                    for (int j = 1; j <= attributeMatcher.groupCount(); j++) {
                        String key = attributeMatcher.group(j);
                        
                        String value = String.valueOf(metric.getAttributes().get(key));
                        
                        metricText = metricText.replaceAll("<attribute:"+key+">", value);
                        
                        j++;
                    }
                }
                
                Matcher attributesMatcher = Pattern.compile("\\<attributes:([^>]+)\\>").matcher(metricText);        
                while (attributesMatcher.find()) {
                    for (int j = 1; j <= attributesMatcher.groupCount(); j++) {
                        String keyPatternAsString = attributesMatcher.group(j);
                        Pattern keyPattern = Pattern.compile(keyPatternAsString);
                        
                        List<Entry<String, String>> matchingAttributes = metric.getAttributes().entrySet().stream()
                                                                                    .filter(entry -> keyPattern.matcher(entry.getKey()).matches())
                                                                                    .collect(Collectors.toList());
                        
                        String quotedKetPattern = Pattern.quote(keyPatternAsString);
                        
                        if(!matchingAttributes.isEmpty()) {
                            StringBuilder sb = new StringBuilder();
                            
                            for(Map.Entry<String, String> att: matchingAttributes)
                                sb.append("\n\t" + att.getKey() + " = " + att.getValue());
                            
                            metricText = metricText.replaceAll("<attributes:"+quotedKetPattern+">", sb.toString());
                        }else {
                            metricText = metricText.replaceAll("<attributes:"+quotedKetPattern+">", "");
                        }
                        
                        j++;
                    }
                }
        		
        		metricText = metricText.replaceAll("<datetime>", dateFormatter.format(metric.getTimestamp()));
        		metricText = metricText.replaceAll("<value>", String.valueOf(metric.getValue()));
        		
        		metricsText += metricText;
			}

			template = preText + metricsText + postText;

        	startAggMetrics = template.indexOf("<agg_metrics>", endAggMetrics);
        }
        
        return template;
    }

}
