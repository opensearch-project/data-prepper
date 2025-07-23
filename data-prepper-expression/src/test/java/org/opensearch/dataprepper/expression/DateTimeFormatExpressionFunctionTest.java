package org.opensearch.dataprepper.expression;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
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

class DateTimeFormatExpressionFunctionTest {
    private final DateTimeFormatExpressionFunction target = new DateTimeFormatExpressionFunction();

    @ParameterizedTest
    @MethodSource("argumentsAndResults")
    void shouldCorrectlyReturnResult(String eventKey, String formatString, String dstTimeZone,  Event input, String result) {
        List<Object> args = Stream.of(eventKey, formatString, dstTimeZone).filter(Objects::nonNull).collect(Collectors.toUnmodifiableList());
        assertThat(target.evaluate(args, input, identity()), is(result));
    }

    private static Event event(final String data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private static Stream<Arguments> argumentsAndResults() {
        return Stream.of(
                Arguments.of("/time", "\"'year='yyyy'/month='MM'/day='dd\"",
                        null,
                        event("{\"time\": " + LocalDateTime.of(2025, 4, 1, 23, 59).toInstant(ZoneOffset.UTC).toEpochMilli() + "}"),
                        "year=2025/month=04/day=01"
                ),
                Arguments.of("/time", "\"'year='yyyy'/month='MM'/day='dd\"",
                        "\"UTC-8\"",
                        event("{\"time\": " + LocalDateTime.of(2025, 4, 1, 23, 59).toInstant(ZoneOffset.UTC).toEpochMilli() + "}"),
                        "year=2025/month=04/day=01"
                )
        );
    }
}