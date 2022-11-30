package io.k8ssandra.metrics.config;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigReaderTest {

    @Test
    void readEmptyConfig() {
        Configuration configuration = ConfigReader.readConfig();
        assertEquals(0, configuration.getFilters().size());
    }

    @Test
    void readFromConfigFile() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("collector.yaml");

        System.setProperty(ConfigReader.CONFIG_PATH_PROPERTY, resource.getFile());
        Configuration configuration = ConfigReader.readConfig();
        assertEquals(2, configuration.getFilters().size());
    }
}
