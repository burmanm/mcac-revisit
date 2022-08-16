package io.k8ssandra.metrics.builder;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.k8ssandra.metrics.prometheus.CassandraDropwizardExports;
import io.prometheus.client.Collector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricsRegisteryTest {

    @Test
    void verifyRegistryListener() {
        MetricRegistry registry = new MetricRegistry();
        CassandraDropwizardExports exporter = new CassandraDropwizardExports(registry);
        int metricsCount = 10;
        for (int i = 0; i < metricsCount; i++) {
            registry.counter(String.format("c_nr_%d", i));
            registry.meter(String.format("m_nr_%d", i));
            registry.timer(String.format("t_nr_%d", i));
            registry.histogram(String.format("h_nr_%d", i));
            registry.register(String.format("g_nr_%d", i), (Gauge<Integer>) () -> 3);
        }

        List<Collector.MetricFamilySamples> collect = exporter.collect();
        int firstCollectSize = collect.size();
        assertEquals(5 * metricsCount, firstCollectSize);
        collect = exporter.collect();
        int secondCollectSize = collect.size();

        assertTrue(firstCollectSize > 0);
        assertEquals(firstCollectSize, secondCollectSize);

        for (int i = 0; i < metricsCount; i++) {
            registry.remove(String.format("c_nr_%d", i));
            registry.remove(String.format("m_nr_%d", i));
            registry.remove(String.format("t_nr_%d", i));
            registry.remove(String.format("h_nr_%d", i));
            registry.remove(String.format("g_nr_%d", i));
        }

        collect = exporter.collect();
        assertEquals(0, collect.size());
    }
}
