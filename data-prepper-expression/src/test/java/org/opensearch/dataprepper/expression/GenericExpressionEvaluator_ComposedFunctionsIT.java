/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.LogEventBuilder;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

class GenericExpressionEvaluator_ComposedFunctionsIT extends BaseExpressionEvaluatorIT {
    @ParameterizedTest
    @CsvSource({
            "2026-03-10T12:00:00,2026,true",
            "2025-03-10T12:00:00,2026,false",
            "2026-03-10T12:00:00,2025,false",
            "2025-03-10T12:00:00,2025,true",
    })
    void evaluate_provides_expected_results_for_composing_startsWith_and_formatDateTime(
            final String dateTimeString, final String startsWithString, boolean expectedValue) {
        final GenericExpressionEvaluator objectUnderTest = applicationContext.getBean(GenericExpressionEvaluator.class);

        long timeInEpochMillis = LocalDateTime.parse(dateTimeString).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        final Event event = TestEventFactory.getTestEventFactory().eventBuilder(LogEventBuilder.class)
                .withData(Map.of("time", timeInEpochMillis))
                .build();

        final String expression = String.format("startsWith(formatDateTime(/time, \"yyyy-MM-dd\"), \"%s\")", startsWithString);
        final Object actualValue = objectUnderTest.evaluate(expression, event);

        assertThat(actualValue, instanceOf(Boolean.class));
        assertThat(actualValue, equalTo(expectedValue));
    }

    @Test
    void evaluate_with_three_levels_using_startsWith_getMetadata_and_formatDateTime() {
        final GenericExpressionEvaluator objectUnderTest = applicationContext.getBean(GenericExpressionEvaluator.class);

        long timeInEpochMillis = LocalDateTime.parse("2026-03-10T12:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        final Event event = TestEventFactory.getTestEventFactory().eventBuilder(LogEventBuilder.class)
                .withData(Map.of("time", timeInEpochMillis))
                .build();
        event.getMetadata().getAttributes().put("2026", "abcde");

        final String expression = "startsWith(getMetadata(formatDateTime(/time, \"yyyy\")), \"abc\")";
        final Object actualValue = objectUnderTest.evaluate(expression, event);

        assertThat(actualValue, instanceOf(Boolean.class));
        assertThat(actualValue, equalTo(true));
    }
}
