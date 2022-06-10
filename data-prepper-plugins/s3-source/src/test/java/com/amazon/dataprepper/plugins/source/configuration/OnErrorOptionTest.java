/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class OnErrorOptionTest {
    @ParameterizedTest
    @EnumSource(OnErrorOption.class)
    void fromOptionValue(final OnErrorOption option) {
        System.out.println(option);
        System.out.println(OnErrorOption.fromOptionValue(option.name()));
        assertThat(OnErrorOption.fromOptionValue(option.name()), is(option));
    }

}