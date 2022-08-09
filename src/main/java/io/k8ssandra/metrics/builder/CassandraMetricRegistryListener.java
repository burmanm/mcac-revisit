package io.k8ssandra.metrics.builder;

import com.codahale.metrics.*;
import io.k8ssandra.metrics.prometheus.CassandraDropwizardExports;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraMetricRegistryListener implements MetricRegistryListener {

    private CassandraMetricNameParser parser;

    private ConcurrentHashMap<String, CassandraMetricDefinition> cache;

    public CassandraMetricRegistryListener(ConcurrentHashMap<String, CassandraMetricDefinition> cache) {
        parser = new CassandraMetricNameParser(CassandraMetricsTools.DEFAULT_LABEL_NAMES, CassandraMetricsTools.DEFAULT_LABEL_VALUES);
        this.cache = cache;
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
        CassandraMetricDefinition sample = parser.parseDropwizardMetric(name, "", new ArrayList<>(), new ArrayList<>());
        cache.put(name, sample);
    }

    @Override
    public void onGaugeRemoved(String name) {
        cache.remove(name);
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
        onGaugeAdded(name, null);
    }

    @Override
    public void onCounterRemoved(String name) {
        cache.remove(name);
    }

    @Override
    public void onHistogramAdded(String dropwizardName, Histogram histogram) {
        for (Double quantile : CassandraDropwizardExports.PRECOMPUTED_QUANTILES) {
            CassandraMetricDefinition sample = parser.parseDropwizardMetric(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList(quantile.toString()));
            String cacheKey = CassandraDropwizardExports.quantileCacheKey(dropwizardName, quantile);
            cache.put(cacheKey, sample);
        }

        CassandraMetricDefinition count = parser.parseDropwizardMetric(dropwizardName, "_count", new ArrayList<>(), new ArrayList<>());
        cache.put(dropwizardName + "_count", count);
    }

    @Override
    public void onHistogramRemoved(String dropwizardName) {
        for (Double quantile : CassandraDropwizardExports.PRECOMPUTED_QUANTILES) {
            cache.remove(CassandraDropwizardExports.quantileCacheKey(dropwizardName, quantile));
        }
        cache.remove(dropwizardName + "_count");
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
        CassandraMetricDefinition total = parser.parseDropwizardMetric(name, "_total", new ArrayList<>(), new ArrayList<>());
        cache.put(name + "_total", total);
    }

    @Override
    public void onMeterRemoved(String name) {
        cache.remove(name + "_total");
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
        onHistogramAdded(name, null);
    }

    @Override
    public void onTimerRemoved(String name) {
        onHistogramRemoved(name);
    }
}
