package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.ServiceMapProcessorConfig.DEFAULT_WINDOW_DURATION;

class ServiceMapProcessorConfigTest {
    private ServiceMapProcessorConfig serviceMapProcessorConfig;
    Random random;

    @BeforeEach
    void setUp() {
        serviceMapProcessorConfig = new ServiceMapProcessorConfig();
        random = new Random();
    }

    @Test
    void testDefaultConfig() {
        assertThat(serviceMapProcessorConfig.getWindowDuration(), equalTo(DEFAULT_WINDOW_DURATION));
        assertThat(serviceMapProcessorConfig.getDbPath(), equalTo(ServiceMapProcessorConfig.DEFAULT_DB_PATH));
    }

    @Test
    void testGetter() throws NoSuchFieldException, IllegalAccessException {
        final int windowDuration = 1 + random.nextInt(300);
        ReflectivelySetField.setField(
                ServiceMapProcessorConfig.class,
                serviceMapProcessorConfig,
                "windowDuration",
                windowDuration);
        final String testDbPath = UUID.randomUUID().toString();
        ReflectivelySetField.setField(
                ServiceMapProcessorConfig.class,
                serviceMapProcessorConfig,
                "dbPath",
                testDbPath);
        assertThat(serviceMapProcessorConfig.getDbPath(), equalTo(testDbPath));
    }
}