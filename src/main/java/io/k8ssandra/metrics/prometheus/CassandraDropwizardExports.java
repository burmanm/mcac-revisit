package io.k8ssandra.metrics.prometheus;

import com.codahale.metrics.Timer;
import com.codahale.metrics.*;
import io.k8ssandra.metrics.builder.CassandraMetricRegistryListener;
import io.k8ssandra.metrics.builder.CassandraMetricDefinition;
import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Collect Dropwizard metrics from CassandraMetricRegistry. This is modified version of the Prometheus' client_java's DropwizardExports
 * to improve performance, parsing and correctness.
 */
public class CassandraDropwizardExports extends io.prometheus.client.Collector implements io.prometheus.client.Collector.Describable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CassandraDropwizardExports.class);
    private MetricRegistry registry;
    private MetricFilter metricFilter;

    private ConcurrentHashMap<String, CassandraMetricDefinition> metricDefinitions;

    public static final Double[] PRECOMPUTED_QUANTILES = new Double[]{0.5, 0.75, 0.95, 0.98, 0.99, 0.999};

    /**
     * Creates a new DropwizardExports with a {@link DefaultSampleBuilder} and {@link MetricFilter#ALL}.
     *
     * @param registry a metric registry to export in prometheus.
     */
    public CassandraDropwizardExports(MetricRegistry registry) {
        this(registry, MetricFilter.ALL);
    }

    /**
     * Creates a new DropwizardExports with a {@link DefaultSampleBuilder} and custom {@link MetricFilter}.
     *
     * @param registry     a metric registry to export in prometheus.
     * @param metricFilter a custom metric filter.
     */
    public CassandraDropwizardExports(MetricRegistry registry, MetricFilter metricFilter) {
        this.registry = registry;
        this.metricFilter = metricFilter;
        this.metricDefinitions = new ConcurrentHashMap<>();
        registry.addListener(new CassandraMetricRegistryListener(this.metricDefinitions));
    }

    private static String getHelpMessage(String metricName, Metric metric) {
        return String.format("Generated from Dropwizard metric import (metric=%s, type=%s)",
                metricName, metric.getClass().getName());
    }

    /**
     * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
     */
    MetricFamilySamples fromCounter(String dropwizardName, Counter counter) {
        MetricFamilySamples.Sample sample = buildSample(dropwizardName, Long.valueOf(counter.getCount()).doubleValue());

        // TODO No point creating a Map here..
        return new MetricFamilySamples(sample.name, Type.GAUGE, getHelpMessage(dropwizardName, counter), Arrays.asList(sample));
    }

    MetricFamilySamples.Sample buildSample(String dropwizardName, double value) {
        CassandraMetricDefinition metricSample = metricDefinitions.get(dropwizardName);

        return buildSample(metricSample, value);
    }

    MetricFamilySamples.Sample buildSample(CassandraMetricDefinition metricSample, double value) {
        return new Collector.MetricFamilySamples.Sample(
                metricSample.getMetricName(),
                metricSample.getLabelNames(),
                metricSample.getLabelValues(),
                value
        );
    }

    /**
     * Export gauge as a prometheus gauge.
     */
    MetricFamilySamples fromGauge(String dropwizardName, Gauge gauge) {
        Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
//            LOGGER.log(Level.FINE, String.format("Invalid type for Gauge %s: %s", sanitizeMetricName(dropwizardName),
//                    obj == null ? "null" : obj.getClass().getName()));
            return null;
        }
        MetricFamilySamples.Sample sample = buildSample(dropwizardName, value);
        return new MetricFamilySamples(sample.name, Type.GAUGE, getHelpMessage(dropwizardName, gauge), Arrays.asList(sample));
    }

    public static String quantileCacheKey(String name, Double quantile) {
        return String.format("%s_q%,.4f", name, quantile);
    }
    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param dropwizardName metric name.
     * @param snapshot       the histogram snapshot.
     * @param count          the total sample count for this snapshot.
     * @param factor         a factor to apply to histogram values.
     */
    MetricFamilySamples fromSnapshotAndCount(String dropwizardName, Snapshot snapshot, long count, double factor, String helpMessage) {
        double values[] = new double[]{
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile()
        };

        List<MetricFamilySamples.Sample> samples = new ArrayList<>(PRECOMPUTED_QUANTILES.length + 1);
        for(int i = 0; i < PRECOMPUTED_QUANTILES.length; i++) {
            String cacheKey = quantileCacheKey(dropwizardName, PRECOMPUTED_QUANTILES[i]);
            CassandraMetricDefinition cassandraMetricDefinition = metricDefinitions.get(cacheKey);
            samples.add(buildSample(cassandraMetricDefinition, values[i] * factor));
        }
        CassandraMetricDefinition countDef = metricDefinitions.get(dropwizardName + "_count");
        samples.add(buildSample(countDef, count));
        return new MetricFamilySamples(samples.get(0).name, Type.SUMMARY, helpMessage, samples);
    }

    /**
     * Convert histogram snapshot.
     */
    MetricFamilySamples fromHistogram(String dropwizardName, Histogram histogram) {
        return fromSnapshotAndCount(dropwizardName, histogram.getSnapshot(), histogram.getCount(), 1.0,
                getHelpMessage(dropwizardName, histogram));
    }

    /**
     * Export Dropwizard Timer as a histogram. Use TIME_UNIT as time unit.
     */
    MetricFamilySamples fromTimer(String dropwizardName, Timer timer) {
        return fromSnapshotAndCount(dropwizardName, timer.getSnapshot(), timer.getCount(),
                1.0D / TimeUnit.SECONDS.toNanos(1L), getHelpMessage(dropwizardName, timer));
    }

    /**
     * Export a Meter as as prometheus COUNTER.
     */
    MetricFamilySamples fromMeter(String dropwizardName, Meter meter) {
        MetricFamilySamples.Sample sample = buildSample(dropwizardName + "_total", meter.getCount());
        return new MetricFamilySamples(sample.name, Type.COUNTER, getHelpMessage(dropwizardName, meter),
                Arrays.asList(sample));
    }

    @Override
    public List<MetricFamilySamples> collect() {
        logger.info("=====> Starting collection of metrics");

        try {
            Map<String, MetricFamilySamples> mfSamplesMap = new HashMap<String, MetricFamilySamples>();

            for (SortedMap.Entry<String, Gauge> entry : registry.getGauges(metricFilter).entrySet()) {
                addToMap(mfSamplesMap, fromGauge(entry.getKey(), entry.getValue()));
            }
            for (SortedMap.Entry<String, Counter> entry : registry.getCounters(metricFilter).entrySet()) {
                addToMap(mfSamplesMap, fromCounter(entry.getKey(), entry.getValue()));
            }
            for (SortedMap.Entry<String, Histogram> entry : registry.getHistograms(metricFilter).entrySet()) {
                addToMap(mfSamplesMap, fromHistogram(entry.getKey(), entry.getValue()));
            }
            for (SortedMap.Entry<String, Timer> entry : registry.getTimers(metricFilter).entrySet()) {
                addToMap(mfSamplesMap, fromTimer(entry.getKey(), entry.getValue()));
            }
            for (SortedMap.Entry<String, Meter> entry : registry.getMeters(metricFilter).entrySet()) {
                addToMap(mfSamplesMap, fromMeter(entry.getKey(), entry.getValue()));
            }
            return new ArrayList<MetricFamilySamples>(mfSamplesMap.values());
        } catch(Exception e) {
            logger.error("Failed to parse metrics", e);
            throw new RuntimeException(e);
        }
    }

    private void addToMap(Map<String, MetricFamilySamples> mfSamplesMap, MetricFamilySamples newMfSamples)
    {
        if (newMfSamples != null) {
            MetricFamilySamples currentMfSamples = mfSamplesMap.get(newMfSamples.name);
            if (currentMfSamples == null) {
                mfSamplesMap.put(newMfSamples.name, newMfSamples);
            } else {
                List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(currentMfSamples.samples);
                samples.addAll(newMfSamples.samples);
                mfSamplesMap.put(newMfSamples.name, new MetricFamilySamples(newMfSamples.name, currentMfSamples.type, currentMfSamples.help, samples));
            }
        }
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return new ArrayList<MetricFamilySamples>();
    }
}