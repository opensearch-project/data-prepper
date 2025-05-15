/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import org.opensearch.dataprepper.model.configuration.PluginModel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.lang.reflect.Field;

public class OcsfProcessorConfigTest {
    
    private OcsfProcessorConfig ocsfProcessorConfig;

    @BeforeEach
    void setUp() {
        ocsfProcessorConfig = new OcsfProcessorConfig();
    }

    @Test
    void TestDefaultConfig() {
        assertThat(ocsfProcessorConfig.getVersion(), equalTo(OcsfProcessorConfig.DEFAULT_VERSION));
        assertThat(ocsfProcessorConfig.getSchemaType(), equalTo(null));
    }

    @Test
    void TestCustomConfig() throws Exception  {
        final String testVersion = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(ocsfProcessorConfig, "version", testVersion);
        assertThat(ocsfProcessorConfig.getVersion(), equalTo(testVersion));
        PluginModel schemaType = mock(PluginModel.class);
        reflectivelySetField(ocsfProcessorConfig, "schemaType", schemaType);
        assertThat(ocsfProcessorConfig.getSchemaType(), equalTo(schemaType));
    }

    private void reflectivelySetField(final OcsfProcessorConfig ocsfProcessorConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = OcsfProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(ocsfProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
    
}
