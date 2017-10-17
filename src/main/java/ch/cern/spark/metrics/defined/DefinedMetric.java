package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;

import ch.cern.ConfigurationException;
import ch.cern.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.Filter;

public class DefinedMetric implements Serializable{

	private static final long serialVersionUID = 82179461944060520L;

	private String name;
	
	private Map<String, Filter> metrics;

	private Set<String> groupByMetricIDs;
	
	private Set<String> metricsWhen;
	
	private Equation equation;

	public DefinedMetric(String name) {
		this.name = name;
	}

	public DefinedMetric config(Properties properties) throws ConfigurationException {
		String equationString = properties.getProperty("value");
		if(equationString == null)
			throw new ConfigurationException("Value must be specified.");
		equation = new Equation(equationString);
		
		String groupByVal = properties.getProperty("metric.groupby");
		if(groupByVal != null)
			groupByMetricIDs = Arrays.stream(groupByVal.split(",")).map(String::trim).collect(Collectors.toSet());
		
		Set<String> metricNames = properties.getSubset("metric").getUniqueKeyFields();
		metrics = metricNames.stream()
			.filter(name -> !name.equals("groupby"))
			.map(name -> new Pair<String, Properties>(name, properties.getSubset("metric").getSubset(name)))
			.map(pair -> new Pair<String, Filter>(pair.first, Filter.build(pair.second.getSubset("filter"))))
			.collect(Collectors.toMap(Pair::first, Pair::second));

		if(metrics.isEmpty())
			throw new ConfigurationException("At least a metric must be described.");
		
		// Equation should be able to compute the result with all metrics
		Optional<Float> resultTest = equation.compute(metrics.keySet().stream()
			.map(name -> new Pair<String, Float>(name, (float) Math.random()))
			.collect(Collectors.toMap(Pair::first, Pair::second)));
		if(!resultTest.isPresent())
			throw new ConfigurationException("Equation (value) contain variables that have not been described.");
		
		metricsWhen = new HashSet<String>();
		String whenValue = properties.getProperty("when");
		if(whenValue != null && whenValue.equals("ANY")) {
			metricsWhen.addAll(metrics.keySet());
		}else if(whenValue != null) {
			Set<String> metricsWhenConfig = Arrays.stream(whenValue.split(",")).map(String::trim).collect(Collectors.toSet());
			
			if(!metrics.keySet().containsAll(metricsWhenConfig))
				throw new ConfigurationException("Metrics listed in when parameter must be declared.");
			
			metricsWhen.addAll(metricsWhenConfig);
		}else{
			metricsWhen.add(metricNames.stream().sorted().findFirst().get());
		}
		
		return this;
	}
	
	public String getName() {
		return name;
	}

	public Equation getEquation() {
		return equation;
	}
	
	protected Map<String, Filter> getMetricsAndFilters(){
		return metrics;
	}
	
	protected Set<String> getMetricsWhen() {
		return metricsWhen;
	}

	public boolean testIfAnyFilter(Metric metric) {
		return metrics.values().stream().filter(filter -> filter.test(metric)).count() > 0;
	}

	public Set<String> getMetricIDs(Metric metric) {
		return metrics.entrySet().stream()
				.filter(entry -> entry.getValue().test(metric))
				.map(Map.Entry::getKey)
				.collect(Collectors.toSet());
	}

	public boolean shouldGenerate(Metric metric) {
		return metrics.entrySet().stream()
				.filter(entry -> metricsWhen.contains(entry.getKey()))
				.map(entry -> entry.getValue())
				.filter(filter -> filter.test(metric))
				.count() > 0;
	}

	public Optional<Metric> generate(DefinedMetricStore store, Metric metric, Map<String, String> groupByMetricIDs) {
		if(!shouldGenerate(metric))
			return Optional.empty();
		
		Optional<Float> value = equation.compute(store.getValues());
		
		if(value.isPresent()) {
			groupByMetricIDs.put("$defined_metric", name);
			
			return Optional.of(new Metric(metric.getInstant(), value.get(), groupByMetricIDs));
		}else {
			return Optional.empty();
		}
	}

	public Map<String, String> getGruopByMetricIDs(Map<String, String> metricIDs) {
		if(groupByMetricIDs == null)
			return new HashMap<>();
		
		if(groupByMetricIDs.contains("ALL"))
			return metricIDs;
		
		return groupByMetricIDs.stream()
					.map(id -> new Pair<String, String>(id, metricIDs.get(id)))
					.filter(pair -> pair.second() != null)
					.collect(Collectors.toMap(Pair::first, Pair::second));
	}

}
