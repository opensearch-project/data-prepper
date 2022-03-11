/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package com.amazon.dataprepper.plugins.processor.drop;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class HandleFailedEventsOptionTest {
    @ParameterizedTest
    @MethodSource("handleFailedEventsOptionProvider")
    void fromOptionValue(final HandleFailedEventsOption option) {
        assertThat(HandleFailedEventsOption.fromOptionValue(option.name()), is(option));
    }

    private static Stream<Arguments> handleFailedEventsOptionProvider() {
        return Arrays.stream(HandleFailedEventsOption.values())
                .map(Arguments::of);
    }

}