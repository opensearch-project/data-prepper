/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class CompressionOptionTest {
    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void fromOptionValue(final CompressionOption option) {
        assertThat(CompressionOption.fromOptionValue(option.name()), is(option));
    }
}