package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Random;

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
    }

    @Test
    void testGetter() throws NoSuchFieldException, IllegalAccessException {
        final int windowDuration = 1 + random.nextInt(300);
        ReflectivelySetField.setField(
                ServiceMapProcessorConfig.class,
                serviceMapProcessorConfig,
                "windowDuration",
                windowDuration);
        assertThat(serviceMapProcessorConfig.getWindowDuration(), equalTo(windowDuration));
    }
}