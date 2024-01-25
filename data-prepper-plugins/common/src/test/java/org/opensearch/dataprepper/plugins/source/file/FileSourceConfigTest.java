/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class FileSourceConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(strings = {FileSourceConfig.EVENT_TYPE, FileSourceConfig.DEFAULT_FORMAT})
    void codeRequiresRecordTypeEvent_returns_true_if_no_codec(final String recordType) {
        final Map<String, String> fileConfigMap = Map.of(FileSourceConfig.ATTRIBUTE_TYPE, recordType);
        final FileSourceConfig objectUnderTest = OBJECT_MAPPER.convertValue(fileConfigMap, FileSourceConfig.class);

        assertThat(objectUnderTest.codeRequiresRecordTypeEvent(), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
            FileSourceConfig.EVENT_TYPE + ",true",
            FileSourceConfig.DEFAULT_FORMAT + ",false"
    })
    void codeRequiresRecordTypeEvent_returns_expected_value_when_there_is_a_codec(final String recordType, final boolean expected) {
        final Map<String, Object> fileConfigMap = Map.of(
                FileSourceConfig.ATTRIBUTE_TYPE, recordType,
                "codec", new PluginModel("fake_codec", Collections.emptyMap())
        );
        final FileSourceConfig objectUnderTest = OBJECT_MAPPER.convertValue(fileConfigMap, FileSourceConfig.class);

        assertThat(objectUnderTest.codeRequiresRecordTypeEvent(), equalTo(expected));
    }
}