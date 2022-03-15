/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.HashMap;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionalExpressionEvaluatorIT {
    /**
     * {@link JacksonEvent#get(String, Class)} supports a String matching the following regex expression:
     * ^[A-Za-z0-9]+([A-Za-z0-9.-_][A-Za-z0-9])*$
     */
    private static final String ALL_JACKSON_EVENT_GET_SUPPORTED_CHARACTERS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-_abcdefghijklmnopqrstuvwxyz0123456789";

    private AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void beforeEach() {
        applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan("org.opensearch.dataprepper.expression");
        applicationContext.refresh();
    }

    @Test
    void testConditionalExpressionEvaluatorBeanAvailable() {
        final ConditionalExpressionEvaluator evaluator = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        assertThat(evaluator, isA(ConditionalExpressionEvaluator.class));
    }

    @Test
    void testConditionalExpressionEvaluatorBeanNotSingleton() {
        final ConditionalExpressionEvaluator instanceA = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        final ConditionalExpressionEvaluator instanceB = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        assertThat(instanceA, not(is(instanceB)));
    }

    @ParameterizedTest
    @MethodSource("validExpressionArguments")
    void testConditionalExpressionEvaluator(final String expression, final Event event, final Boolean expected) {
        final ConditionalExpressionEvaluator evaluator = applicationContext.getBean(ConditionalExpressionEvaluator.class);

        final Boolean actual = evaluator.evaluate(expression, event);

        assertThat(actual, is(expected));
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionArguments")
    void testConditionalExpressionEvaluatorThrows(final String expression, final Event event) {
        final ConditionalExpressionEvaluator evaluator = applicationContext.getBean(ConditionalExpressionEvaluator.class);

        assertThrows(RuntimeException.class, () -> evaluator.evaluate(expression, event));
    }

    private static Stream<Arguments> validExpressionArguments() {
        return Stream.of(
                Arguments.of("true", event("{}"), true),
                Arguments.of("/status_code == 200", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code != 300", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code == 200", event("{}"), false),
                Arguments.of("/success == /status_code", event("{\"success\": true, \"status_code\": 200}"), false),
                Arguments.of("/success != /status_code", event("{\"success\": true, \"status_code\": 200}"), true),
                Arguments.of("/pi == 3.14159", event("{\"pi\": 3.14159}"), true),
                Arguments.of("true == (/is_cool == true)", event("{\"is_cool\": true}"), true),
                Arguments.of("not /is_cool", event("{\"is_cool\": true}"), false),
                Arguments.of("/status_code < 300", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code <= 0", event("{\"status_code\": 200}"), false),
                Arguments.of("/status_code > 0", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code >= 300", event("{\"status_code\": 200}"), false),
                Arguments.of("-/status_code == -200", event("{\"status_code\": 200}"), true),
                Arguments.of("/success and /status_code == 200", event("{\"success\": true, \"status_code\": 200}"), true),
                Arguments.of("/success or /status_code == 200", event("{\"success\": false, \"status_code\": 200}"), true),
                Arguments.of("(/success == true) or (/status_code == 200)", event("{\"success\": false, \"status_code\": 200}"), true),
                Arguments.of("/should_drop", event("{\"should_drop\": true}"), true),
                Arguments.of("/should_drop", event("{\"should_drop\": false}"), false),
                Arguments.of("/logs/2/should_drop", event("{\"logs\": [{}, {}, {\"should_drop\": true}]}"), true),
                Arguments.of(
                        escapedJsonPointer(ALL_JACKSON_EVENT_GET_SUPPORTED_CHARACTERS) + " == true",
                        complexEvent(ALL_JACKSON_EVENT_GET_SUPPORTED_CHARACTERS, true),
                        true)
        );
    }

    private static Stream<Arguments> invalidExpressionArguments() {
        return Stream.of(
                Arguments.of("/missing", event("{}")),
                Arguments.of("/success < /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success <= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success > /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success >= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("not /status_code", event("{\"status_code\": 200}")),
                Arguments.of("/status_code >= 200 and 3", event("{\"status_code\": 200}")),
                Arguments.of("", event("{}")),
                Arguments.of("-false", event("{}")),
                Arguments.of("not 5", event("{}")),
                Arguments.of("not/status_code", event("{\"status_code\": 200}")),
                Arguments.of("trueand/status_code", event("{\"status_code\": 200}")),
                Arguments.of("trueor/status_code", event("{\"status_code\": 200}"))
        );
    }

    private static String escapedJsonPointer(final String pointer) {
        return "\"/" + pointer + "\"";
    }

    private static Event event(final String data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private static <T> Event complexEvent(final String key, final T value) {
        final HashMap<String, T> data = new HashMap<>();
        data.put(key, value);

        return JacksonEvent.builder()
                .withEventType("event")
                .withData(data)
                .build();
    }
}