/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class FolderPartitioningOptionsTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void defaults_have_expected_values() {
        final FolderPartitioningOptions options = new FolderPartitioningOptions();
        assertThat(options.getFolderDepth(), equalTo(1));
        assertThat(options.getMaxObjectsPerOwnership(), equalTo(50));
        assertThat(options.getIdleScanMode(), equalTo(IdleScanMode.IDLE_OPTIMIZED));
        assertThat(options.isContinuousScanMode(), equalTo(false));
        assertThat(options.getEmptyFolderRescanDelay(), equalTo(Duration.ZERO));
    }

    @Test
    void isContinuousScanMode_returns_true_when_continuous() throws Exception {
        final String json = "{\"idle_scan_mode\": \"continuous\"}";
        final FolderPartitioningOptions options = objectMapper.readValue(json, FolderPartitioningOptions.class);
        assertThat(options.isContinuousScanMode(), equalTo(true));
        assertThat(options.getIdleScanMode(), equalTo(IdleScanMode.CONTINUOUS));
    }

    @Test
    void isContinuousScanMode_returns_false_when_idle_optimized() throws Exception {
        final String json = "{\"idle_scan_mode\": \"idle_optimized\"}";
        final FolderPartitioningOptions options = objectMapper.readValue(json, FolderPartitioningOptions.class);
        assertThat(options.isContinuousScanMode(), equalTo(false));
    }

    @Test
    void getIdleScanMode_defaults_to_idle_optimized_when_null() {
        final FolderPartitioningOptions options = new FolderPartitioningOptions();
        assertThat(options.getIdleScanMode(), equalTo(IdleScanMode.IDLE_OPTIMIZED));
    }

    @Test
    void isContinuousScanMode_returns_false_when_idle_scan_mode_is_null() {
        final FolderPartitioningOptions options = new FolderPartitioningOptions();
        assertThat(options.isContinuousScanMode(), equalTo(false));
    }

    @Test
    void getEmptyFolderRescanDelay_returns_configured_value() throws Exception {
        final String json = "{\"empty_folder_rescan_delay\": \"PT1M\"}";
        final FolderPartitioningOptions options = objectMapper.readValue(json, FolderPartitioningOptions.class);
        assertThat(options.getEmptyFolderRescanDelay(), equalTo(Duration.ofMinutes(1)));
    }

    @Test
    void getEmptyFolderRescanDelay_returns_zero_when_null() throws Exception {
        final String json = "{\"empty_folder_rescan_delay\": null}";
        final FolderPartitioningOptions options = objectMapper.readValue(json, FolderPartitioningOptions.class);
        assertThat(options.getEmptyFolderRescanDelay(), equalTo(Duration.ZERO));
    }
}
