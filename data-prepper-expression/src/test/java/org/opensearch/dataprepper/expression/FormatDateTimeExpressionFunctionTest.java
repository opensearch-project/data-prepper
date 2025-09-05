/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.expression;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class FormatDateTimeExpressionFunctionTest {
    private final FormatDateTimeExpressionFunction target = new FormatDateTimeExpressionFunction();

    @ParameterizedTest
    @MethodSource("functionArgumentsAndExpectedResults")
    void shouldCorrectlyReturnResult(String formatString, String dstTimeZone, String sourceTimeZone, Event input, String result) {
        List<Object> args = Stream.of("/time", formatString, dstTimeZone, sourceTimeZone).filter(Objects::nonNull).collect(Collectors.toUnmodifiableList());
        assertThat(target.evaluate(args, input, identity()), is(result));
    }

    private static Event eventWithTime(final Object data) {
        JacksonEvent result = JacksonEvent.builder().withEventType("event").build();
        result.put("time", data);
        return result;
    }

    private static Stream<Arguments> functionArgumentsAndExpectedResults() {
        return Stream.of(
                arguments("\"'year='yyyy'/month='MM'/day='dd\"",
                        null,
                        null,
                        eventWithTime(LocalDateTime.of(2025, 4, 1, 23, 59).toInstant(ZoneOffset.UTC).toEpochMilli()),
                        "year=2025/month=04/day=01"
                ),
                arguments("\"'year='yyyy'/month='MM'/day='dd\"",
                        "\"UTC-8\"",
                        null,
                        eventWithTime("2025-04-01T23:59:00"),
                        "year=2025/month=04/day=01"
                ),
                arguments("\"yyyy-MM-dd HH:mm:ss\"",
                        "\"UTC\"",
                        "\"UTC\"",
                        eventWithTime(Instant.parse("2025-06-15T14:30:45Z").toEpochMilli()),
                        "2025-06-15 14:30:45"
                ),
                arguments("\"yyyy-MM-dd HH:mm:ss\"",
                        "\"America/New_York\"",
                        "\"UTC\"",
                        eventWithTime(Instant.parse("2025-06-15T18:30:45Z").toEpochMilli()),
                        "2025-06-15 14:30:45"
                ),
                arguments("\"yyyy-MM-dd HH:mm:ss\"",
                        "\"America/New_York\"",
                        "\"UTC+5\"",
                        eventWithTime("2025-06-15T18:30:45"), //no zone in the string, so source timezone takes place
                        "2025-06-15 09:30:45"
                ),
                arguments("\"dd/MM/yyyy HH:mm\"",
                        "\"UTC\"",
                        "\"UTC\"",
                        eventWithTime(LocalDateTime.of(2025, 3, 20, 9, 15).toInstant(ZoneOffset.UTC).toEpochMilli()),
                        "20/03/2025 09:15"
                ),
                arguments("\"dd/MM/yyyy HH:mm\"",
                        "\"Europe/London\"",
                        "\"UTC\"",
                        eventWithTime(LocalDateTime.of(2025, 8, 20, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli()),
                        "20/08/2025 13:00"
                ),
                arguments("\"yyyy-MM-dd'T'HH:mm:ssXXX\"",
                        "\"UTC\"",
                        "\"UTC\"",
                        eventWithTime(OffsetDateTime.parse("2025-12-25T10:30:00+02:00").toInstant().toEpochMilli()),
                        "2025-12-25T08:30:00Z"
                ),
                arguments("\"MM/dd/yyyy HH:mm\"",
                        "\"America/Los_Angeles\"",
                        "\"UTC\"",
                        eventWithTime(OffsetDateTime.parse("2025-07-04T16:45:30-05:00").toInstant().toEpochMilli()),
                        "07/04/2025 14:45"
                ),
                arguments("\"yyyy-MM-dd HH:mm:ss z\"",
                        "\"UTC\"",
                        "\"UTC\"",
                        eventWithTime(ZonedDateTime.parse("2025-09-10T22:15:30+03:00[Europe/Moscow]").toInstant().toEpochMilli()),
                        "2025-09-10 19:15:30 UTC"
                ),
                arguments("\"MMM dd, yyyy h:mm a\"",
                        "\"Asia/Tokyo\"",
                        "\"UTC\"",
                        eventWithTime(ZonedDateTime.parse("2025-01-01T12:00:00Z[UTC]").toInstant().toEpochMilli()),
                        "Jan 01, 2025 9:00 PM"
                ),
                arguments("\"yyyy-MM-dd HH:mm:ss\"",
                        "\"Europe/Paris\"",
                        "\"America/Chicago\"",
                        eventWithTime(LocalDateTime.of(2025, 6, 15, 14, 30).atZone(ZoneId.of("America/Chicago")).toInstant().toEpochMilli()),
                        "2025-06-15 21:30:00"
                ),
                arguments("\"HH:mm:ss\"",
                        "\"Australia/Sydney\"",
                        "\"Europe/Berlin\"",
                        eventWithTime(OffsetDateTime.parse("2025-04-10T08:00:00+02:00").toInstant().toEpochMilli()),
                        "16:00:00"
                )
        );
    }
}