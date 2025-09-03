/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig.DEFAULT_SOURCE_COORDINATION_STORE;

class SourceCoordinationConfigTest {

    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    @Test
    void getDefaultSourceCoordinationConfig_returns_expected_values() {
        final SourceCoordinationConfig sourceCoordinationConfig = SourceCoordinationConfig.getDefaultSourceCoordinationConfig();

        assertThat(sourceCoordinationConfig, notNullValue());
        assertThat(sourceCoordinationConfig.getSourceCoordinationStoreConfig(), notNullValue());
        assertThat(sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName(), equalTo(DEFAULT_SOURCE_COORDINATION_STORE));
        assertThat(sourceCoordinationConfig.getPartitionPrefix(), nullValue());
        assertThat(sourceCoordinationConfig.getLeaseTimeout(), equalTo(SourceCoordinationConfig.DEFAULT_LEASE_TIMEOUT));
    }

    @Test
    void constructor_with_all_values() {
        final String pluginName = UUID.randomUUID().toString();
        final String partitionPrefix = UUID.randomUUID().toString();
        final Duration leaseTimeout = Duration.ofSeconds(random.nextInt(100) + 1);
        final SourceCoordinationConfig sourceCoordinationConfig = new SourceCoordinationConfig(
                new PluginModel(pluginName, Collections.emptyMap()),
                partitionPrefix,
                leaseTimeout
        );

        assertThat(sourceCoordinationConfig, notNullValue());
        assertThat(sourceCoordinationConfig.getSourceCoordinationStoreConfig(), notNullValue());
        assertThat(sourceCoordinationConfig.getSourceCoordinationStoreConfig().getName(), equalTo(pluginName));
        assertThat(sourceCoordinationConfig.getPartitionPrefix(), equalTo(partitionPrefix));
        assertThat(sourceCoordinationConfig.getLeaseTimeout(), equalTo(leaseTimeout));
    }
}