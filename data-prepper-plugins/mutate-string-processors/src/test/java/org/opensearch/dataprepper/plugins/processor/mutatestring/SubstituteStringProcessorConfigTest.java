/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class SubstituteStringProcessorConfigTest {
    @Mock
    private EventKey eventKey;

    @ParameterizedTest
    @MethodSource("provideFromAndIsValid")
    void testIsFromValid(final String from, final boolean isValid) {
        final SubstituteStringProcessorConfig.Entry objectUnderTest = new SubstituteStringProcessorConfig.Entry(
                eventKey, from, null, null);
        assertThat(objectUnderTest.isFromValid(), is(isValid));
    }

    private static Stream<Arguments> provideFromAndIsValid() {
        return Stream.of(
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}