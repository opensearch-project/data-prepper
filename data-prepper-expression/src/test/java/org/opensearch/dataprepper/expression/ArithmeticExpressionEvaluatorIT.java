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
import static org.hamcrest.CoreMatchers.not;
import org.apache.commons.lang3.RandomStringUtils;

class ArithmeticExpressionEvaluatorIT {

    private AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void beforeEach() {
        applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan("org.opensearch.dataprepper.expression");
        applicationContext.refresh();
    }

    @Test
    void testArithmeticExpressionEvaluatorBeanAvailable() {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);
        assertThat(evaluator, isA(GenericExpressionEvaluator.class));
    }

    @Test
    void testArithmeticExpressionEvaluatorBeanSingleton() {
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
    void testArithmeticExpressionEvaluator(final String expression, final Event event, final Number expected, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final Number actual = (Number)evaluator.evaluate(expression, event);

        assertThat(actual, is(expected));
        assertThat(actual, instanceOf(expectedClass));
    }

    @ParameterizedTest
    @MethodSource("validExpressionArguments")
    void testArithmeticExpressionEvaluatorWithMultipleThreads(final String expression, final Event event, final Number expected, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final int numberOfThreads = 50;
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        List<Number> evaluationResults = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(() -> evaluationResults.add((Number)evaluator.evaluate(expression, event)));
        }

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> evaluationResults.size() == numberOfThreads);

        assertThat(evaluationResults.size(), equalTo(numberOfThreads));
        for (Number evaluationResult : evaluationResults) {
            assertThat(evaluationResult, equalTo(expected));
            assertThat(evaluationResult, instanceOf(expectedClass));
        }
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionArguments")
    void testArithmeticExpressionEvaluatorInvalidInput(final String expression, final Event event, final Class expectedClass) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);
        Object result = evaluator.evaluate(expression, event);
        assertThat(result, not(instanceOf(expectedClass)));
    }

    private static Stream<Arguments> validExpressionArguments() {
        Random random = new Random();
        int randomInt = random.nextInt(10000);
        float randomFloat = random.nextFloat();
        int testStringLength = random.nextInt(30);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        return Stream.of(
                Arguments.of(""+randomInt, event("{}"), randomInt, Integer.class),
                Arguments.of("/status_code", event("{\"status_code\": "+randomInt+"}"), randomInt, Integer.class),
                Arguments.of("/status_code", event("{\"status_code\": "+randomFloat+"}"), randomFloat, Float.class),
                Arguments.of("length(/message)", event("{\"message\": \""+testString+"\"}"), testString.length(), Integer.class)
        );
    }

    private static Stream<Arguments> invalidExpressionArguments() {
        Random random = new Random();
        int testStringLength = random.nextInt(10);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        return Stream.of(
                Arguments.of("/missing", event("{}"), Integer.class),
                Arguments.of("/message", event("{\"message\": \""+testString+"\"}"), Integer.class),
                Arguments.of("/status", event("{\"status\": true}"), Integer.class),
                Arguments.of("/status", event("{\"status\": 5.55}"), Integer.class),
                Arguments.of("/status", event("{\"status\": 200}"), Float.class)
        );
    }

    private static Event event(final String data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }
}
