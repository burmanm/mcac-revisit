package io.k8ssandra.metrics.builder;

import io.prometheus.client.Collector;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

public class CassandraMetricDefinition {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraMetricDefinition.class);

    private final List<String> labelNames;
    private final List<String> labelValues;
    private final String metricName;
    private Supplier<Double> valueGetter;

    public CassandraMetricDefinition(String metricName, List<String> labelNames, List<String> labelValues, Supplier<Double> valueGetter) {
        this.labelNames = labelNames;
        this.labelValues = labelValues;
        this.valueGetter = valueGetter;
        this.metricName = metricName;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

    public List<String> getLabelValues() {
        return labelValues;
    }

    public String getMetricName() {
        return metricName;
    }

    void setValueGetter(Supplier<Double> valueGetter) {
        this.valueGetter = valueGetter;
    }

    public Collector.MetricFamilySamples.Sample buildSample() {
        return new Collector.MetricFamilySamples.Sample(
                getMetricName(),
                getLabelNames(),
                getLabelValues(),
                valueGetter.get()
        );
    }
}
