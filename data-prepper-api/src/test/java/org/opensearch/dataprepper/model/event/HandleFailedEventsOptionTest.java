/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HandleFailedEventsOptionTest {
    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void fromOptionValue(final HandleFailedEventsOption option) {
        assertThat(HandleFailedEventsOption.fromOptionValue(option.name()), CoreMatchers.is(option));

        if (option == HandleFailedEventsOption.SKIP || option == HandleFailedEventsOption.SKIP_SILENTLY) {
            assertThat(option.shouldDropEvent(), equalTo(false));
        } else {
            assertThat(option.shouldDropEvent(), equalTo(true));
        }

        if (option == HandleFailedEventsOption.SKIP_SILENTLY || option == HandleFailedEventsOption.DROP_SILENTLY) {
            assertThat(option.shouldLog(), equalTo(false));
        } else {
            assertThat(option.shouldLog(), equalTo(true));
        }
    }
}
