package io.k8ssandra.metrics.benchmark;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.k8ssandra.metrics.prometheus.CassandraDropwizardExports;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class MetricRegistryParsingBenchmark {

    private MetricRegistry registry;

    private CassandraDropwizardExports exporter;

    @Param({ "1000" })
    private int metricsCount;

    @Setup(Level.Trial)
    public void init() throws Exception {
        registry = new MetricRegistry();
        exporter = new CassandraDropwizardExports(registry);
        for(int i = 0; i < metricsCount; i++) {
            registry.counter(String.format("c_nr_%d", i));
            registry.meter(String.format("m_nr_%d", i));
            registry.timer(String.format("t_nr_%d", i));
            registry.histogram(String.format("h_nr_%d", i));

            registry.register(String.format("g_nr_%d", i), (Gauge<Integer>) () -> 3);
            registry.register(String.format("gh_nr_%d", i), (Gauge<long[]>) () -> new long[]{1,2,3,0});
        }
    }

    @Benchmark
    public void registryParsing(Blackhole bh) {
        bh.consume(exporter.collect());
    }
}
