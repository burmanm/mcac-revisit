package io.k8ssandra.metrics.builder;

import com.codahale.metrics.*;
import io.prometheus.client.Collector;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.k8ssandra.metrics.builder.CassandraMetricsTools.PRECOMPUTED_QUANTILES;
import static io.k8ssandra.metrics.builder.CassandraMetricsTools.PRECOMPUTED_QUANTILES_TEXT;

public class CassandraMetricRegistryListener implements MetricRegistryListener {

    private CassandraMetricNameParser parser;

    private final ConcurrentHashMap<String, RefreshableMetricFamilySamples> familyCache;

    // This cache is used for the remove purpose, we need dropwizardName -> metricName mapping
    private ConcurrentHashMap<String, String> cache;

    public CassandraMetricRegistryListener(ConcurrentHashMap<String, RefreshableMetricFamilySamples> familyCache) {
        parser = new CassandraMetricNameParser(CassandraMetricsTools.DEFAULT_LABEL_NAMES, CassandraMetricsTools.DEFAULT_LABEL_VALUES);
        cache = new ConcurrentHashMap<>();
        this.familyCache = familyCache;
    }

    public void updateCache(String dropwizardName, String metricName, RefreshableMetricFamilySamples prototype) {
        RefreshableMetricFamilySamples familySamples;
        if(!familyCache.containsKey(metricName)) {
            familyCache.put(metricName, prototype);
            familySamples = prototype;
        } else {
            familySamples = familyCache.get(metricName);
        }

        familySamples.getCollectionSuppliers().putAll(prototype.getCollectionSuppliers());
        prototype.getDefinitions().forEach(familySamples::addDefinition);
        cache.put(dropwizardName, metricName);
    }

    public void removeFromCache(String dropwizardName) {
        String metricName = cache.get(dropwizardName);
        RefreshableMetricFamilySamples familySampler = familyCache.get(metricName);

        familySampler.removeCollectionSupplier(dropwizardName);
        familySampler.getDefinitions().removeIf(cmd -> cmd.getMetricName().equals(metricName));

        if ((familySampler.getCollectionSuppliers().size() + familySampler.getDefinitions().size()) < 1) {
            this.familyCache.remove(metricName);
        }
        cache.remove(dropwizardName);
    }

    @NotNull
    private static Supplier<List<Collector.MetricFamilySamples.Sample>> getGaugeHistogramSupplier(Gauge gauge, CassandraMetricDefinition proto, CassandraMetricDefinition count) {
        return () -> {
            if(gauge.getValue() == null) {
                return new ArrayList<>();
            }
            long[] inputValues = (long[]) gauge.getValue();
            if (inputValues.length == 0) {
                // Empty
                return new ArrayList<>();
            }

            final EstimatedHistogram hist = new EstimatedHistogram(inputValues);
            List<Collector.MetricFamilySamples.Sample> samples = new ArrayList<>(PRECOMPUTED_QUANTILES.length + 1);
            for(int i = 0; i < PRECOMPUTED_QUANTILES.length; i++) {
                List<String> labelValues = new ArrayList<>(proto.getLabelValues());
                labelValues.add(PRECOMPUTED_QUANTILES_TEXT[i]);
                Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(
                        proto.getMetricName(),
                        proto.getLabelNames(),
                        labelValues,
                        hist.percentile(PRECOMPUTED_QUANTILES[i]));
                samples.add(sample);
            }

            // _count
            count.setValueGetter(() -> (double) hist.count());
            samples.add(count.buildSample());

            return samples;
        };
    }

    private Supplier<Double> fromGauge(final Gauge<?> gauge) {
        return () -> {
            Object obj = gauge.getValue();
            double value;
            if (obj instanceof Number) {
                value = ((Number) obj).doubleValue();
            } else if (obj instanceof Boolean) {
                value = ((Boolean) obj) ? 1 : 0;
            } else {
                // These are of type "HashMap<?, ?> and ArrayList<?>", but I haven't found any with actual data on my tests. Add specific parsing
                // later if we find out something valuable is missing.
                return 0.0;
            }

            return value;
        };
    }

    @Override
    public void onGaugeAdded(String dropwizardName, Gauge<?> gauge) {
        if(gauge.getValue() instanceof long[]) {
            // Treat this as a histogram, not gauge
            final CassandraMetricDefinition proto = parser.parseDropwizardMetric(dropwizardName, "", Arrays.asList("quantile"), new ArrayList<>(), null);
            final CassandraMetricDefinition count = parser.parseDropwizardMetric(dropwizardName, "_count", new ArrayList<>(), new ArrayList<>(), null);

            Supplier<List<Collector.MetricFamilySamples.Sample>> supplySamples = getGaugeHistogramSupplier(gauge, proto, count);

            RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(proto.getMetricName(), Collector.Type.SUMMARY, "", new ArrayList<>());
            familySamples.addCollectionSupplier(dropwizardName, supplySamples);

            updateCache(dropwizardName, proto.getMetricName(), familySamples);
            return;
        }
        CassandraMetricDefinition sample = parser.parseDropwizardMetric(dropwizardName, "", new ArrayList<>(), new ArrayList<>(), fromGauge(gauge));
        RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(sample.getMetricName(), Collector.Type.GAUGE, "", new ArrayList<>());
        familySamples.addDefinition(sample);
        updateCache(dropwizardName, sample.getMetricName(), familySamples);
    }

    @Override
    public void onGaugeRemoved(String name) {
        removeFromCache(name);
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
        Supplier<Double> getValue = () -> Long.valueOf(counter.getCount()).doubleValue();
        CassandraMetricDefinition sampler = parser.parseDropwizardMetric(name, "", new ArrayList<>(), new ArrayList<>(), getValue);
        RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(sampler.getMetricName(), Collector.Type.GAUGE, "", new ArrayList<>());
        familySamples.addDefinition(sampler);
        updateCache(name, sampler.getMetricName(), familySamples);
    }

    @Override
    public void onCounterRemoved(String name) {
        removeFromCache(name);
    }

    @Override
    public void onHistogramAdded(String dropwizardName, Histogram histogram) {
        // TODO Do we want extra processing for DecayingHistogram and EstimatedHistograms?

        final CassandraMetricDefinition proto = parser.parseDropwizardMetric(dropwizardName, "", Arrays.asList("quantile"), new ArrayList<>(), () -> 0.0);
        final CassandraMetricDefinition count = parser.parseDropwizardMetric(dropwizardName, "_count", new ArrayList<>(), new ArrayList<>(), () -> (double) histogram.getCount());

        Supplier<List<Collector.MetricFamilySamples.Sample>> supplySamples = getHistogramSupplier(histogram, proto, count, 1.0);

        RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(proto.getMetricName(), Collector.Type.SUMMARY, "", new ArrayList<>());
        familySamples.addCollectionSupplier(dropwizardName, supplySamples);

        updateCache(dropwizardName, proto.getMetricName(), familySamples);
    }

    @NotNull
    private static Supplier<List<Collector.MetricFamilySamples.Sample>> getHistogramSupplier(Histogram histogram, CassandraMetricDefinition proto, CassandraMetricDefinition count, double factor) {
        return () -> {
            Snapshot snapshot = histogram.getSnapshot();
            double[] values = new double[]{
                    snapshot.getMedian(),
                    snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(),
                    snapshot.get98thPercentile(),
                    snapshot.get99thPercentile(),
                    snapshot.get999thPercentile()
            };
            List<Collector.MetricFamilySamples.Sample> samples = new ArrayList<>(PRECOMPUTED_QUANTILES.length + 1);
            for(int i = 0; i < PRECOMPUTED_QUANTILES.length; i++) {
                List<String> labelValues = new ArrayList<>(proto.getLabelValues());
                labelValues.add(PRECOMPUTED_QUANTILES_TEXT[i]);
                Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(
                        proto.getMetricName(),
                        proto.getLabelNames(),
                        labelValues,
                        values[i] * factor);
                samples.add(sample);
            }
            samples.add(count.buildSample());

            return samples;
        };
    }

    @Override
    public void onHistogramRemoved(String dropwizardName) {
        String metricName = cache.get(dropwizardName);

        RefreshableMetricFamilySamples familySamples = familyCache.get(metricName);
        familySamples.removeCollectionSupplier(metricName);

        removeFromCache(dropwizardName);
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
        Supplier<Double> getValue = () -> (double) meter.getCount();
        CassandraMetricDefinition total = parser.parseDropwizardMetric(name, "_total", new ArrayList<>(), new ArrayList<>(), getValue);
        RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(total.getMetricName(), Collector.Type.COUNTER, "", new ArrayList<>());
        familySamples.addDefinition(total);
        updateCache(name, total.getMetricName(), familySamples);
    }

    @Override
    public void onMeterRemoved(String name) {
        removeFromCache(name);
    }

    @NotNull
    private static Supplier<List<Collector.MetricFamilySamples.Sample>> getTimerSupplier(Timer timer, CassandraMetricDefinition proto, CassandraMetricDefinition count, double factor) {
        Supplier<List<Collector.MetricFamilySamples.Sample>> supplySamples = () -> {
            Snapshot snapshot = timer.getSnapshot();
            double[] values = new double[]{
                    snapshot.getMedian(),
                    snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(),
                    snapshot.get98thPercentile(),
                    snapshot.get99thPercentile(),
                    snapshot.get999thPercentile()
            };
            List<Collector.MetricFamilySamples.Sample> samples = new ArrayList<>(PRECOMPUTED_QUANTILES.length + 1);
            for(int i = 0; i < PRECOMPUTED_QUANTILES.length; i++) {
                List<String> labelValues = new ArrayList<>(proto.getLabelValues());
                labelValues.add(PRECOMPUTED_QUANTILES_TEXT[i]);
                Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(
                        proto.getMetricName(),
                        proto.getLabelNames(),
                        labelValues,
                        values[i] * factor);
                samples.add(sample);
            }
            samples.add(count.buildSample());

            return samples;
        };
        return supplySamples;
    }

    @Override
    public void onTimerAdded(String dropwizardName, Timer timer) {
        double factor = 1.0D / TimeUnit.SECONDS.toNanos(1L);
        final CassandraMetricDefinition proto = parser.parseDropwizardMetric(dropwizardName, "", Arrays.asList("quantile"), new ArrayList<>(), () -> 0.0);
        final CassandraMetricDefinition count = parser.parseDropwizardMetric(dropwizardName, "_count", new ArrayList<>(), new ArrayList<>(), () -> (double) timer.getCount());

        Supplier<List<Collector.MetricFamilySamples.Sample>> supplySamples = getTimerSupplier(timer, proto, count, factor);

        RefreshableMetricFamilySamples familySamples = new RefreshableMetricFamilySamples(proto.getMetricName(), Collector.Type.SUMMARY, "", new ArrayList<>());
        familySamples.addCollectionSupplier(dropwizardName, supplySamples);

        updateCache(dropwizardName, proto.getMetricName(), familySamples);
    }

    @Override
    public void onTimerRemoved(String name) {
        onHistogramRemoved(name);
    }
}
