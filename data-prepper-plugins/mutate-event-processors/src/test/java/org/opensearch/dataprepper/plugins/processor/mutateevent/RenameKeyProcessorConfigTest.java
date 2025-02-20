/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

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
class RenameKeyProcessorConfigTest {
    @Mock
    private EventKey eventKey;

    @ParameterizedTest
    @MethodSource("provideFromKeyRegexAndIsValid")
    void testIsFromKeyRegexValid(final String fromKeyRegex, final boolean isValid) {
        final RenameKeyProcessorConfig.Entry objectUnderTest = new RenameKeyProcessorConfig.Entry(
                null, fromKeyRegex, eventKey, false, null);
        assertThat(objectUnderTest.isFromKeyRegexValid(), is(isValid));
    }

    private static Stream<Arguments> provideFromKeyRegexAndIsValid() {
        return Stream.of(
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}