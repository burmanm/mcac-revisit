package io.k8ssandra.metrics.builder;

import io.prometheus.client.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class RefreshableMetricFamilySamples extends Collector.MetricFamilySamples {
    private final List<CassandraMetricDefinition> definitions;

    private final Map<String, Consumer<List<Sample>>> listFillers;

    public RefreshableMetricFamilySamples(String name, Collector.Type type, String help, List<Sample> samples) {
        super(name, type, help, samples);
        definitions = new CopyOnWriteArrayList<>();
        listFillers = new ConcurrentHashMap<>();
    }

    public void refreshSamples() {
        // Fetch all linked metricDefinitions
        samples.clear();
        for (CassandraMetricDefinition definition : definitions) {
            samples.add(definition.buildSample());
        }
        for(Consumer<List<Sample>> filler : listFillers.values()) {
            filler.accept(samples);
        }
    }

    public void addDefinition(CassandraMetricDefinition definition) {
        definitions.add(definition);
    }

    public void addSampleFiller(String name, Consumer<List<Sample>> filler) {
        listFillers.put(name, filler);
    }

    public void removeSampleFiller(String name) {
        listFillers.remove(name);
    }

    public List<CassandraMetricDefinition> getDefinitions() {
        return definitions;
    }

    public Map<String, Consumer<List<Sample>>> getListFillers() {
        return listFillers;
    }
}
