/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void tail_defaults_to_false() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.isTail(), equalTo(false));
    }

    @Test
    void tail_can_be_set_to_true() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log", "tail", true);
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.isTail(), equalTo(true));
    }

    @Test
    void paths_defaults_to_empty_list() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getPaths(), empty());
    }

    @Test
    void paths_returns_configured_values() {
        final Map<String, Object> configMap = Map.of("paths", List.of("/var/log/*.log", "/tmp/*.log"));
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getPaths(), hasSize(2));
    }

    @Test
    void getAllPaths_merges_path_and_paths() {
        final Map<String, Object> configMap = Map.of(
                "path", "/tmp/single.log",
                "paths", List.of("/var/log/*.log")
        );
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getAllPaths(), hasSize(2));
        assertThat(config.getAllPaths(), containsInAnyOrder("/var/log/*.log", "/tmp/single.log"));
    }

    @Test
    void getAllPaths_deduplicates_when_path_is_in_paths() {
        final Map<String, Object> configMap = Map.of(
                "path", "/var/log/*.log",
                "paths", List.of("/var/log/*.log")
        );
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getAllPaths(), hasSize(1));
    }

    @Test
    void validate_succeeds_with_path_when_tail_false() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        config.validate();
    }

    @Test
    void validate_fails_without_path_when_tail_false() {
        final Map<String, Object> configMap = Map.of("format", "plain", "record_type", "string");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThrows(NullPointerException.class, config::validate);
    }

    @Test
    void validate_succeeds_with_paths_when_tail_true() {
        final Map<String, Object> configMap = Map.of("tail", true, "paths", List.of("/var/log/*.log"));
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        config.validate();
    }

    @Test
    void validate_succeeds_with_path_when_tail_true() {
        final Map<String, Object> configMap = Map.of("tail", true, "path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        config.validate();
    }

    @Test
    void validate_fails_without_any_path_when_tail_true() {
        final Map<String, Object> configMap = Map.of("tail", true);
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void default_config_returns_expected_values() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getStartPosition(), equalTo(StartPosition.END));
        assertThat(config.getPollInterval(), equalTo(Duration.ofSeconds(1)));
        assertThat(config.getEncoding(), equalTo("UTF-8"));
        assertThat(config.getReadBufferSize(), equalTo(65536));
        assertThat(config.getMaxActiveFiles(), equalTo(100));
        assertThat(config.getReaderThreads(), equalTo(2));
        assertThat(config.getMaxReadTimePerFile(), equalTo(Duration.ofSeconds(5)));
        assertThat(config.getRotateWait(), equalTo(Duration.ofSeconds(5)));
        assertThat(config.getRotationDrainTimeout(), equalTo(Duration.ofSeconds(30)));
        assertThat(config.getCheckpointFile(), nullValue());
        assertThat(config.getCheckpointInterval(), equalTo(Duration.ofSeconds(15)));
        assertThat(config.getCheckpointCleanupAfter(), equalTo(Duration.ofHours(24)));
        assertThat(config.getFingerprintBytes(), equalTo(1024));
        assertThat(config.getCloseInactive(), equalTo(Duration.ofMinutes(5)));
        assertThat(config.isCloseRemoved(), equalTo(true));
        assertThat(config.getBatchSize(), equalTo(1000));
        assertThat(config.getBatchTimeout(), equalTo(Duration.ofSeconds(5)));
        assertThat(config.getAcknowledgmentTimeout(), equalTo(Duration.ofSeconds(30)));
        assertThat(config.getMaxAcknowledgmentRetries(), equalTo(3));
        assertThat(config.isIncludeFileMetadata(), equalTo(false));
        assertThat(config.getMaxLineLength(), equalTo(1048576));
    }

    @Test
    void exclude_paths_defaults_to_empty_list() {
        final Map<String, Object> configMap = Map.of("path", "/tmp/test.log");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getExcludePaths(), empty());
    }

    @Test
    void exclude_paths_returns_configured_values() {
        final Map<String, Object> configMap = Map.of(
                "path", "/tmp/test.log",
                "exclude_paths", List.of("/tmp/exclude*.log")
        );
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getExcludePaths(), hasSize(1));
    }

    @Test
    void getAllPaths_with_null_filePathToRead_returns_only_paths() {
        final Map<String, Object> configMap = Map.of(
                "paths", List.of("/var/log/*.log")
        );
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThat(config.getAllPaths(), hasSize(1));
        assertThat(config.getAllPaths(), containsInAnyOrder("/var/log/*.log"));
    }

    @Test
    void validate_fails_when_tail_true_and_filePathToRead_is_empty_and_paths_is_null() {
        final Map<String, Object> configMap = Map.of("tail", true, "path", "");
        final FileSourceConfig config = OBJECT_MAPPER.convertValue(configMap, FileSourceConfig.class);

        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void getFormat_returns_plain_when_format_is_null() {
        assertThat(FileFormat.getByName(null), equalTo(FileFormat.PLAIN));
    }
}
