package io.k8ssandra.metrics.builder;

import io.prometheus.client.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

public class RefreshableMetricFamilySamples extends Collector.MetricFamilySamples {
    private List<CassandraMetricDefinition> definitions;
    private Map<String, Supplier<List<Sample>>> collectionSuppliers;

    public RefreshableMetricFamilySamples(String name, Collector.Type type, String help, List<Sample> samples) {
        super(name, type, help, samples);
        definitions = new CopyOnWriteArrayList<>();
        collectionSuppliers = new ConcurrentHashMap<>();
    }

    public void refreshSamples() {
        // Fetch all linked metricDefinitions
        samples.clear();
        for (CassandraMetricDefinition definition : definitions) {
            samples.add(definition.buildSample());
        }
        for (Supplier<List<Sample>> listSupplier : collectionSuppliers.values()) {
            samples.addAll(listSupplier.get());
        }
    }

    public void addDefinition(CassandraMetricDefinition definition) {
        definitions.add(definition);
    }

    public int removeDefinition(CassandraMetricDefinition definition) {
        definitions.remove(definition);
        return definitions.size();
    }

    public void addCollectionSupplier(String name, Supplier<List<Sample>> supplier) {
        collectionSuppliers.put(name, supplier);
    }

    public void removeCollectionSupplier(String name) {
        collectionSuppliers.remove(name);
    }

    public List<CassandraMetricDefinition> getDefinitions() {
        return definitions;
    }

    public Map<String, Supplier<List<Sample>>> getCollectionSuppliers() {
        return collectionSuppliers;
    }
}
