/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.MatcherAssert.assertThat;

class HandleFailedEventsOptionTest {
    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void fromOptionValue(final HandleFailedEventsOption option) {
        assertThat(HandleFailedEventsOption.fromOptionValue(option.name()), CoreMatchers.is(option));
    }
}
