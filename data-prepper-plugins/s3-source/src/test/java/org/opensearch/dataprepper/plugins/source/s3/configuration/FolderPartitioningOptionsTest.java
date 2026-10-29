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
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class FolderPartitioningOptionsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void defaults_have_expected_values() {
        final FolderPartitioningOptions options = new FolderPartitioningOptions();
        assertThat(options.getFolderDepth(), equalTo(1));
        assertThat(options.getMaxObjectsPerOwnership(), equalTo(50));
        assertThat(options.getIdleScanMode(), equalTo(IdleScanMode.IDLE_OPTIMIZED));
        assertThat(options.isContinuousScanMode(), equalTo(false));
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
}
