/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RateLimiterModeTest {

    @ParameterizedTest
    @EnumSource(RateLimiterMode.class)
    void fromOptionValue(final RateLimiterMode value) {
        assertThat(RateLimiterMode.fromOptionValue(value.name()), is(value));
        assertThat(value, instanceOf(RateLimiterMode.class));
    }
}
