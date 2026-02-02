/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class OtelApmServiceMapProcessorConfigTest {
    
    private OtelApmServiceMapProcessorConfig otelApmServiceMapProcessorConfig;
    
    OtelApmServiceMapProcessorConfig createObjectUnderTest() {
        return new OtelApmServiceMapProcessorConfig();
    }

    @Test
    public void testConfigDefaults() {
        otelApmServiceMapProcessorConfig = createObjectUnderTest();
        assertThat(otelApmServiceMapProcessorConfig.getWindowDuration(), equalTo(Duration.ofSeconds(OtelApmServiceMapProcessorConfig.DEFAULT_WINDOW_DURATION_SECONDS)));
        assertThat(otelApmServiceMapProcessorConfig.getDbPath(), equalTo(OtelApmServiceMapProcessorConfig.DEFAULT_DB_PATH));
        assertThat(otelApmServiceMapProcessorConfig.getGroupByAttributes(), equalTo(Collections.emptyList()));
    }

    @Test
    public void testCustomConfigValues() throws NoSuchFieldException, IllegalAccessException {
        final Duration TEST_WINDOW_DURATION = Duration.ofSeconds(100);
        final String TEST_DB_PATH = UUID.randomUUID().toString();
        final List<String> TEST_ATTRIBUTES = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        otelApmServiceMapProcessorConfig = createObjectUnderTest();
        ReflectivelySetField.setField(OtelApmServiceMapProcessorConfig.class, otelApmServiceMapProcessorConfig, "windowDuration", TEST_WINDOW_DURATION);
        ReflectivelySetField.setField(OtelApmServiceMapProcessorConfig.class, otelApmServiceMapProcessorConfig, "dbPath", TEST_DB_PATH);
        ReflectivelySetField.setField(OtelApmServiceMapProcessorConfig.class, otelApmServiceMapProcessorConfig, "groupByAttributes", TEST_ATTRIBUTES);
        
        assertThat(otelApmServiceMapProcessorConfig.getWindowDuration(), equalTo(TEST_WINDOW_DURATION));
        assertThat(otelApmServiceMapProcessorConfig.getDbPath(), equalTo(TEST_DB_PATH));
        assertThat(otelApmServiceMapProcessorConfig.getGroupByAttributes(), equalTo(TEST_ATTRIBUTES));
    }
}
