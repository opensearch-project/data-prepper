/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class SnappyCompressionOptionTest {
    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    @CsvSource({"SNAPPY"})
    void fromOptionValue(final CompressionOption option) {
        assertThat(CompressionOption.fromOptionValue(option.name()), is(option));
        assertThat(option.getEngine(), instanceOf(CompressionEngine.class));
    }
}