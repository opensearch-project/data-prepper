/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logging;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Marker;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;
import static org.opensearch.dataprepper.logging.SensitiveArgumentMaskingConverter.MASK_PATTERN;

@ExtendWith(MockitoExtension.class)
class SensitiveArgumentMaskingConverterTest {
    @Mock
    private LogEvent logEvent;
    @Mock
    private Message message;
    @Mock
    private org.apache.logging.log4j.Marker log4jMarker;

    @Test
    void testNewInstance() {
        assertThat(SensitiveArgumentMaskingConverter.newInstance().getName(), equalTo("sensitiveArgument"));
    }

    @ParameterizedTest
    @MethodSource("provideSensitiveMarker")
    void testFormatAppliedToLogEventWithSensitiveMarkers(final Marker marker) {
        final SensitiveArgumentMaskingConverter objectUnderTest = SensitiveArgumentMaskingConverter.newInstance();
        when(log4jMarker.getName()).thenReturn(marker.getName());
        when(logEvent.getMarker()).thenReturn(log4jMarker);
        when(logEvent.getMessage()).thenReturn(message);
        when(message.getFormat()).thenReturn("Test log message with anchor: {}");
        when(message.getParameters()).thenReturn(new Object[] {"value"});
        final StringBuilder stringBuilder = new StringBuilder();
        objectUnderTest.format(logEvent, stringBuilder);
        assertThat(stringBuilder.toString(), equalTo(
                String.format("Test log message with anchor: %s", MASK_PATTERN)));
    }

    @Test
    void testFormatAppliedToLogEventWithOtherMarker() {
        final SensitiveArgumentMaskingConverter objectUnderTest = SensitiveArgumentMaskingConverter.newInstance();
        final String testMarkerName = "unknown-" + RandomStringUtils.randomAlphabetic(5);
        when(log4jMarker.getName()).thenReturn(testMarkerName);
        when(logEvent.getMarker()).thenReturn(log4jMarker);
        when(logEvent.getMessage()).thenReturn(message);
        final String testFormattedMessage = "Test log message";
        when(message.getFormattedMessage()).thenReturn(testFormattedMessage);
        final StringBuilder stringBuilder = new StringBuilder();
        objectUnderTest.format(logEvent, stringBuilder);
        assertThat(stringBuilder.toString(), equalTo(testFormattedMessage));
        verify(message, times(0)).getFormat();
        verify(message, times(0)).getParameters();
    }

    @Test
    void testFormatAppliedToLogEventWithoutMarker() {
        final SensitiveArgumentMaskingConverter objectUnderTest = SensitiveArgumentMaskingConverter.newInstance();
        when(logEvent.getMarker()).thenReturn(null);
        when(logEvent.getMessage()).thenReturn(message);
        final String testFormattedMessage = "Test log message";
        when(message.getFormattedMessage()).thenReturn(testFormattedMessage);
        final StringBuilder stringBuilder = new StringBuilder();
        objectUnderTest.format(logEvent, stringBuilder);
        assertThat(stringBuilder.toString(), equalTo(testFormattedMessage));
        verify(message, times(0)).getFormat();
        verify(message, times(0)).getParameters();
    }

    @ParameterizedTest
    @MethodSource("provideSensitiveLogMessageBuildingBlocksAndResult")
    void testFormatSensitiveStringBuildCornerCases(final String format, final Object[] parameters, final String result) {
        final SensitiveArgumentMaskingConverter objectUnderTest = SensitiveArgumentMaskingConverter.newInstance();
        when(logEvent.getMarker()).thenReturn(log4jMarker);
        when(log4jMarker.getName()).thenReturn(SENSITIVE.getName());
        when(logEvent.getMessage()).thenReturn(message);
        when(message.getFormat()).thenReturn(format);
        when(message.getParameters()).thenReturn(parameters);
        final StringBuilder stringBuilder = new StringBuilder();
        objectUnderTest.format(logEvent, stringBuilder);
        assertThat(stringBuilder.toString(), equalTo(result));
    }

    private static Stream<Arguments> provideSensitiveMarker() {
        return Stream.of(
                Arguments.of(EVENT),
                Arguments.of(SENSITIVE)
        );
    }

    public static Stream<Arguments> provideSensitiveLogMessageBuildingBlocksAndResult() {
        return Stream.of(
                Arguments.of("Insufficient anchors: {}", new Object[]{"event1", "event2"},
                        String.format("Insufficient anchors: %s", MASK_PATTERN)),
                Arguments.of("Message with anchors and exception: {}", new Object[]{
                        "event1", new RuntimeException()},
                        String.format("Message with anchors and exception: %s", MASK_PATTERN)),
                Arguments.of("Overflow anchors: {} {}", new Object[]{"event1"},
                        String.format("Overflow anchors: %s {}", MASK_PATTERN)),
                Arguments.of("Escaped anchor: \\{}", new Object[]{"event1"},
                        "Escaped anchor: {}")
        );
    }
}