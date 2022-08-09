package io.k8ssandra.metrics.builder;

import java.util.List;

public class CassandraMetricDefinition {
    private final List<String> labelNames;
    private final List<String> labelValues;
    private final String metricName;

    public CassandraMetricDefinition(String metricName, List<String> labelNames, List<String> labelValues) {
        this.labelNames = labelNames;
        this.labelValues = labelValues;
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
}
