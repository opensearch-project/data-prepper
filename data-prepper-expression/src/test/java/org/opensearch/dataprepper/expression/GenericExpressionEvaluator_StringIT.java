/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.Random;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.commons.lang3.RandomStringUtils;

class GenericExpressionEvaluator_StringIT {

    private AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void beforeEach() {
        applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan("org.opensearch.dataprepper.expression");
        applicationContext.refresh();
    }

    @Test
    void testStringExpressionEvaluatorBeanAvailable() {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);
        assertThat(evaluator, isA(GenericExpressionEvaluator.class));
    }

    @Test
    void testStringExpressionEvaluatorBeanSingleton() {
        final GenericExpressionEvaluator instanceA = applicationContext.getBean(GenericExpressionEvaluator.class);
        final GenericExpressionEvaluator instanceB = applicationContext.getBean(GenericExpressionEvaluator.class);
        assertThat(instanceA, sameInstance(instanceB));
    }

    @Test
    void testParserBeanInstanceOfMultiThreadParser() {
        final Parser instance = applicationContext.getBean(Parser.class);
        assertThat(instance, instanceOf(MultiThreadParser.class));
    }

    @Test
    void testSingleThreadParserBeanNotSingleton() {
        final Parser instanceA = applicationContext.getBean(ParseTreeParser.SINGLE_THREAD_PARSER_NAME, Parser.class);
        final Parser instanceB = applicationContext.getBean(ParseTreeParser.SINGLE_THREAD_PARSER_NAME, Parser.class);
        assertThat(instanceA, not(sameInstance(instanceB)));
    }

    @ParameterizedTest
    @MethodSource("validExpressionArguments")
    void testStringExpressionEvaluator(final String expression, final Event event, final String expected, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final String actual = (String)evaluator.evaluate(expression, event);

        assertThat(actual, is(expected));
        assertThat(actual, instanceOf(expectedClass));
    }

    @ParameterizedTest
    @MethodSource("validExpressionArguments")
    void testStringExpressionEvaluatorWithMultipleThreads(final String expression, final Event event, final String expected, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final int numberOfThreads = 50;
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        List<String> evaluationResults = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(() -> evaluationResults.add((String)evaluator.evaluate(expression, event)));
        }

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> evaluationResults.size() == numberOfThreads);

        assertThat(evaluationResults.size(), equalTo(numberOfThreads));
        for (String evaluationResult : evaluationResults) {
            assertThat(evaluationResult, equalTo(expected));
            assertThat(evaluationResult, instanceOf(expectedClass));
        }
    }

    @ParameterizedTest
    @MethodSource("exceptionExpressionArguments")
    void testArithmeticExpressionEvaluatorInvalidInput(final String expression, final Event event) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);
        assertThrows(ExpressionEvaluationException.class, () -> evaluator.evaluate(expression, event));
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionArguments")
    void testStringExpressionEvaluatorInvalidInput(final String expression, final Event event, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final Object result = evaluator.evaluate(expression, event);
        assertThat(result, not(instanceOf(expectedClass)));
    }

    private static Stream<Arguments> validExpressionArguments() {
        Random random = new Random();
        int testStringLength = random.nextInt(30);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        String testString2 = RandomStringUtils.randomAlphabetic(testStringLength);
        Map<String, Object> attributes = Map.of("strAttr", testString);
        String testData = "{\"key\": \"value\"}";
        JacksonEvent testEvent =  JacksonEvent.builder().withEventType("event").withEventMetadataAttributes(attributes).withData(testData).build();
        return Stream.of(
                Arguments.of("\""+testString+"\"", event("{}"), testString, String.class),
                Arguments.of("/status_message", event("{\"status_message\": \""+testString+"\"}"), testString, String.class),
                Arguments.of("\""+testString+"\"+\""+testString2+"\"", event("{}"), testString+testString2, String.class),
                Arguments.of("/status_message+/message", event("{\"status_message\": \""+testString+"\", \"message\":\""+testString2+"\"}"), testString+testString2, String.class),
                Arguments.of("getMetadata(\"strAttr\")+\""+testString2+"\"+/key", testEvent, testString+testString2+"value", String.class)
        );
    }

    private static Stream<Arguments> invalidExpressionArguments() {
        Random random = new Random();
        int testStringLength = random.nextInt(10);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        int randomInt = random.nextInt(10000);
        return Stream.of(
                Arguments.of("/missing", event("{}"), String.class),
                Arguments.of("/value", event("{\"value\": "+randomInt+"}"), String.class),
                Arguments.of("length(/message)", event("{\"message\": \""+testString+"\"}"), String.class)
        );
    }

    private static Stream<Arguments> exceptionExpressionArguments() {
        return Stream.of(
                // Can't mix Numbers and Strings when using operators
                Arguments.of("/status + /message", event("{\"status\": 200, \"message\":\"msg\"}"))
        );
    }

    private static Event event(final String data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }
}

