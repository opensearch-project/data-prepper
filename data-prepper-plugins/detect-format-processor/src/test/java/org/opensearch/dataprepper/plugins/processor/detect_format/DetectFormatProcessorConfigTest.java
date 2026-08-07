/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.detect_format;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.UUID;

public class DetectFormatProcessorConfigTest {
    private DetectFormatProcessorConfig detectFormatProcessorConfig;

    DetectFormatProcessorConfig createObjectUnderTest() {
        return new DetectFormatProcessorConfig();
    }

    @BeforeEach
    void setup() {
        detectFormatProcessorConfig = createObjectUnderTest();
    }

    @Test
    public void testDefaults() {
        assertThat(detectFormatProcessorConfig.getSource(), equalTo(null));
        assertThat(detectFormatProcessorConfig.getTargetKey(), equalTo(null));
        assertThat(detectFormatProcessorConfig.getTargetMetadataKey(), equalTo(null));
        assertThat(detectFormatProcessorConfig.getKVDelimiter(), equalTo(DetectFormatProcessorConfig.DEFAULT_KV_DELIMITER));
        assertThat(detectFormatProcessorConfig.getKVSeparatorList(), equalTo(DetectFormatProcessorConfig.DEFAULT_KV_SEPARATOR_LIST));
    }

    @Test
    public void testCustomConfigValues() throws NoSuchFieldException, IllegalAccessException {
        final String testSource = UUID.randomUUID().toString();
        ReflectivelySetField.setField(detectFormatProcessorConfig.getClass(), detectFormatProcessorConfig, "source", testSource);
        assertThat(detectFormatProcessorConfig.getSource(), equalTo(testSource));
        final String testTargetKey = UUID.randomUUID().toString();
        ReflectivelySetField.setField(detectFormatProcessorConfig.getClass(), detectFormatProcessorConfig, "targetKey", testTargetKey);
        assertThat(detectFormatProcessorConfig.getTargetKey(), equalTo(testTargetKey));
        ReflectivelySetField.setField(detectFormatProcessorConfig.getClass(), detectFormatProcessorConfig, "targetMetadataKey", testTargetKey);
        assertThat(detectFormatProcessorConfig.getTargetMetadataKey(), equalTo(testTargetKey));
        final String testKVDelimiter = UUID.randomUUID().toString();
        ReflectivelySetField.setField(detectFormatProcessorConfig.getClass(), detectFormatProcessorConfig, "kvDelimiter", testKVDelimiter);
        assertThat(detectFormatProcessorConfig.getKVDelimiter(), equalTo(testKVDelimiter));
        final List<String> testKVSeparatorList = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        ReflectivelySetField.setField(detectFormatProcessorConfig.getClass(), detectFormatProcessorConfig, "kvSeparatorList", testKVSeparatorList);
        assertThat(detectFormatProcessorConfig.getKVSeparatorList(), equalTo(testKVSeparatorList));
    
    }

}
