package io.k8ssandra.metrics.builder.filter;

import io.k8ssandra.metrics.builder.CassandraMetricDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SingleFilterTests {
    /**
         - source_labels: [__name__]
           separator: "@"
           regex: "org_apache_cassandra_metrics_table_.*"
           action: "drop"
     */
    @Test
    public void TestDropWithName() {
        FilteringSpec spec = new FilteringSpec(List.of("__name__"), "", "org_apache_cassandra_metrics_table_.*", "drop");
        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(spec));

        CassandraMetricDefinition tableDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "peers_v2"));

        assertFalse(filter.matches(tableDefinition, "org.apache.cassandra.metrics.Table.RangeLatency"));

        CassandraMetricDefinition keyspaceDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system"));

        assertTrue(filter.matches(keyspaceDefinition, "org.apache.cassandra.metrics.Keyspace.RangeLatency"));
    }

    /**
     - source_labels: [__name__, table]
       separator: "@"
       regex: "(org_apache_cassandra_metrics_table_.*)@dropped_columns"
       action: "keep"
     */
    @Test
    public void TestDropWithNameLabelCombo() {
        FilteringSpec spec = new FilteringSpec(List.of("__name__", "table"), "@", "(org_apache_cassandra_metrics_table_.*)@dropped_columns", "keep");
        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(spec));

        CassandraMetricDefinition tableDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "peers_v2"));
        assertFalse(filter.matches(tableDefinition, "org.apache.cassandra.metrics.Table.RangeLatency"));

        CassandraMetricDefinition tableDefinitionKeep = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_estimated_partition_size_histogram",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "dropped_columns"));
        assertTrue(filter.matches(tableDefinitionKeep, "org.apache.cassandra.metrics.Keyspace.RangeLatency"));

        CassandraMetricDefinition keyspaceDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system"));

        assertFalse(filter.matches(keyspaceDefinition, "org.apache.cassandra.metrics.Keyspace.RangeLatency"));
    }

    /**
     * Drop all table metrics, except those with label table=dropped_columns
     * Don't drop other metrics

     - source_labels: [__name__, table]
       separator: "@"
       regex: "(org_apache_cassandra_metrics_table_.*)@\b(?!dropped_columns\b)\w+"
       action: "drop"
     */
    @Test
    public void TestDropWithNameLabelComboWithExcept() {
        FilteringSpec spec = new FilteringSpec(List.of("__name__", "table"), "@", "(org_apache_cassandra_metrics_table_.*)@\\b(?!dropped_columns\\b)\\w+", "drop");
        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(spec));

        CassandraMetricDefinition tableDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "peers_v2"));
        assertFalse(filter.matches(tableDefinition, "org.apache.cassandra.metrics.Table.RangeLatency"));

        CassandraMetricDefinition tableDefinitionKeep = new CassandraMetricDefinition("org_apache_cassandra_metrics_table_estimated_partition_size_histogram",
                List.of("host", "cluster", "datacenter", "rack", "keyspace", "table"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system", "dropped_columns"));
        assertTrue(filter.matches(tableDefinitionKeep, "org.apache.cassandra.metrics.Keyspace.RangeLatency"));

        CassandraMetricDefinition keyspaceDefinition = new CassandraMetricDefinition("org_apache_cassandra_metrics_keyspace_range_latency_count",
                List.of("host", "cluster", "datacenter", "rack", "keyspace"),
                List.of("6cc2e5ce-e73f-4592-8d02-fd5e17a070e3", "Test Cluster", "dc1", "rack1", "system"));

        assertTrue(filter.matches(keyspaceDefinition, "org.apache.cassandra.metrics.Keyspace.RangeLatency"));
    }

    @Test
    public void OnlyMatchingLabel() {
        FilteringSpec tableLabelFilter = new FilteringSpec(List.of("table"), "@", ".+", "drop");
        CassandraMetricDefinitionFilter filter = new CassandraMetricDefinitionFilter(List.of(tableLabelFilter));

        CassandraMetricDefinition hasTableLabel = new CassandraMetricDefinition("has_table_label", List.of("table"), List.of("value"));
        assertFalse(filter.matches(hasTableLabel, ""));

        CassandraMetricDefinition hasNoLabels = new CassandraMetricDefinition("has_table_label", List.of(), List.of());
        assertTrue(filter.matches(hasNoLabels, ""));

        CassandraMetricDefinition hasOtherLabels = new CassandraMetricDefinition("has_table_label", List.of("keyspace"), List.of("value"));
        assertTrue(filter.matches(hasOtherLabels, ""));
    }

}
