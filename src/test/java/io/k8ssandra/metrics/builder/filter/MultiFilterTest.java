package io.k8ssandra.metrics.builder.filter;

import io.k8ssandra.metrics.builder.CassandraMetricDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiFilterTest {

    @Test
    public void DropMultipleMetrics() {
        // drop jvm_classes_loaded
        FilteringSpec dropJVM = new FilteringSpec(List.of("__name__"), "", "jvm_classes_loaded.*", "drop");

        // drop table metrics
        FilteringSpec spec = new FilteringSpec(List.of("__name__"), "", "org_apache_cassandra_metrics_table_.*", "drop");

        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(dropJVM, spec));

        CassandraMetricDefinition tableDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "peers_v2"));

        CassandraMetricDefinition keyspaceDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system"));

        CassandraMetricDefinition jvmDefinition = new CassandraMetricDefinition("jvm_classes_loaded_total", List.of(), List.of());

        List<CassandraMetricDefinition> definitions = List.of(tableDefinition, keyspaceDefinition, jvmDefinition);
        List<CassandraMetricDefinition> passed = new ArrayList<>(1);

        for (CassandraMetricDefinition definition : definitions) {
            if(filter.matches(definition, "")) {
                passed.add(definition);
            }
        }

        assertEquals(1, passed.size());
    }

    @Test
    public void KeepAndDropSubset() {
        // Keep only production cluster metrics
        FilteringSpec clusterFilter = new FilteringSpec(List.of("cluster"), "@", "production", "keep");

        // But drop all with table label
        FilteringSpec tableLabelFilter = new FilteringSpec(List.of("table"), "@", ".+", "drop");

        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(clusterFilter, tableLabelFilter));

        CassandraMetricDefinition tableDefinitionTest = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "peers_v2"));

        CassandraMetricDefinition tableDefinitionProd = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "production", "dc1", "rack1", "system", "system_schema"));

        CassandraMetricDefinition keyspaceDefinitionProd = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "production", "dc1", "rack1", "system"));

        CassandraMetricDefinition keyspaceDefinitionTest = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system"));

        List<CassandraMetricDefinition> definitions = List.of(tableDefinitionTest, tableDefinitionProd, keyspaceDefinitionProd, keyspaceDefinitionTest);
        List<CassandraMetricDefinition> passed = new ArrayList<>(1);

        for (CassandraMetricDefinition definition : definitions) {
            if(filter.matches(definition, "")) {
                passed.add(definition);
            }
        }

        assertEquals(1, passed.size());
        assertEquals("org_apache_cassandra_metrics_keyspace_range_latency_count", passed.get(0).getMetricName());
        assertEquals("production", passed.get(0).getLabelValues().get(1));
    }
}
