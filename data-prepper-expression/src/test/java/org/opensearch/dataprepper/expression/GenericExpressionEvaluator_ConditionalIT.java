/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.apache.commons.lang3.RandomStringUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenericExpressionEvaluator_ConditionalIT {
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
    void testGenericExpressionEvaluatorBeanAvailable() {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);
        assertThat(evaluator, isA(GenericExpressionEvaluator.class));
    }

    @Test
    void testGenericExpressionEvaluatorBeanSingleton() {
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
    void testConditionalExpressionEvaluator(final String expression, final Event event, final Boolean expected) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final Boolean actual = evaluator.evaluateConditional(expression, event);

        assertThat(actual, is(expected));
    }

    @ParameterizedTest
    @MethodSource("validExpressionArguments")
    void testGenericExpressionEvaluatorWithMultipleThreads(final String expression, final Event event, final Boolean expected) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        final int numberOfThreads = 10;
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        List<Boolean> evaluationResults = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(() -> evaluationResults.add(evaluator.evaluateConditional(expression, event)));
        }

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> evaluationResults.size() == numberOfThreads);

        assertThat(evaluationResults.size(), equalTo(numberOfThreads));
        for (Boolean evaluationResult : evaluationResults) {
            assertThat(evaluationResult, equalTo(expected));
        }
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionArguments")
    void testGenericExpressionEvaluatorThrows(final String expression, final Event event) {
        final GenericExpressionEvaluator evaluator = applicationContext.getBean(GenericExpressionEvaluator.class);

        assertThrows(RuntimeException.class, () -> evaluator.evaluateConditional(expression, event));
    }

    private static Stream<Arguments> validExpressionArguments() {
        final String key = "status_code";
        final Long value = 200L;
        final String strValue = RandomStringUtils.randomAlphabetic(5);
        final int value4 = 2000;
        final Boolean value5 = false;
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        final Map<String, Object> attributesMap = Map.of("key1", strValue, "key2", value4, "key3", value5);
        Event longEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .withEventMetadataAttributes(attributesMap)
                .build();

        String testTag1 = RandomStringUtils.randomAlphabetic(6);
        String testTag2 = RandomStringUtils.randomAlphabetic(7);
        String testTag3 = RandomStringUtils.randomAlphabetic(6);
        String testTag4 = RandomStringUtils.randomAlphabetic(7);

        longEvent.getMetadata().addTags(List.of(testTag1, testTag2, testTag3));

        Random random = new Random();
        int testStringLength = random.nextInt(10);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        return Stream.of(
                Arguments.of("true", event("{}"), true),
                Arguments.of("/status_code == 200", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code == 200", longEvent, true),
                Arguments.of("/status_code != 300", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code == 200", event("{}"), false),
                Arguments.of("/success == /status_code", event("{\"success\": true, \"status_code\": 200}"), false),
                Arguments.of("/success != /status_code", event("{\"success\": true, \"status_code\": 200}"), true),
                Arguments.of("/pi == 3.14159", event("{\"pi\": 3.14159}"), true),
                Arguments.of("/value == 12345.678", event("{\"value\": 12345.678}"), true),
                Arguments.of("/value == 12345.678E12", event("{\"value\": 12345.678E12}"), true),
                Arguments.of("/value == 12345.678e-12", event("{\"value\": 12345.678e-12}"), true),
                Arguments.of("/value == 12345.0000012", event("{\"value\": 12345.0000012}"), true),
                Arguments.of("/value == 12345.00012E6", event("{\"value\": 12345.00012E6}"), true),
                Arguments.of("true == (/is_cool == true)", event("{\"is_cool\": true}"), true),
                Arguments.of("not /is_cool", event("{\"is_cool\": true}"), false),
                Arguments.of("/status_code < 300", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code != null", event("{\"status_code\": 200}"), true),
                Arguments.of("null != /status_code", event("{\"status_code\": 200}"), true),
                Arguments.of("/status_code == null", event("{\"status_code\": null}"), true),
                Arguments.of("/response == null", event("{\"status_code\": 200}"), true),
                Arguments.of("null == /response", event("{\"status_code\": 200}"), true),
                Arguments.of("/response != null", event("{\"status_code\": 200}"), false),
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
                        true),
                Arguments.of("/durationInNanos > 5000000000", event("{\"durationInNanos\": 6000000000}"), true),
                Arguments.of("/response == \"OK\"", event("{\"response\": \"OK\"}"), true),
                Arguments.of("length(/response) == "+testStringLength, event("{\"response\": \""+testString+"\"}"), true),
                Arguments.of("hasTags(\""+ testTag1+"\")", longEvent, true),
                Arguments.of("hasTags(\""+ testTag1+"\",\""+testTag2+"\")", longEvent, true),
                Arguments.of("hasTags(\""+ testTag1+"\", \""+testTag2+"\", \""+testTag3+"\")", longEvent, true),
                Arguments.of("hasTags(\""+ testTag4+"\")", longEvent, false),
                Arguments.of("hasTags(\""+ testTag3+"\",\""+testTag4+"\")", longEvent, false),
                Arguments.of("contains(\""+ strValue+"\",\""+strValue.substring(1,5)+"\")", longEvent, true),
                Arguments.of("contains(/status,\""+strValue.substring(0,2)+"\")", event("{\"status\":\""+strValue+"\"}"), true),
                Arguments.of("contains(\""+strValue+strValue+"\",/status)", event("{\"status\":\""+strValue+"\"}"), true),
                Arguments.of("contains(/message,/status)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), true),
                Arguments.of("contains(/unknown,/status)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), false),
                Arguments.of("contains(/status,/unknown)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), false),
                Arguments.of("getMetadata(\"key1\") == \""+strValue+"\"", longEvent, true),
                Arguments.of("getMetadata(\"key2\") == "+value4, longEvent, true),
                Arguments.of("getMetadata(\"key3\") == "+value5, longEvent, true),
                Arguments.of("getMetadata(\"/key1\") == \""+strValue+"\"", longEvent, true),
                Arguments.of("getMetadata(\"/key2\") == "+value4, longEvent, true),
                Arguments.of("getMetadata(\"key3\") == "+value5, longEvent, true),
                Arguments.of("getMetadata(\"/key6\") == \""+value5+"\"", longEvent, false),
                Arguments.of("getMetadata(\"key6\") == "+value5, longEvent, false),
                Arguments.of("cidrContains(/sourceIp,\"192.0.2.0/24\")", event("{\"sourceIp\": \"192.0.2.3\"}"), true),
                Arguments.of("cidrContains(/sourceIp,\"192.0.2.0/24\",\"192.1.1.0/24\")", event("{\"sourceIp\": \"192.0.2.3\"}"), true),
                Arguments.of("cidrContains(/sourceIp,\"192.0.2.0/24\",\"192.1.1.0/24\")", event("{\"sourceIp\": \"192.2.2.3\"}"), false),
                Arguments.of("cidrContains(/sourceIp,\"2001:0db8::/32\")", event("{\"sourceIp\": \"2001:0db8:aaaa:bbbb::\"}"), true),
                Arguments.of("cidrContains(/sourceIp,\"2001:0db8::/32\",\"2001:aaaa::/32\")", event("{\"sourceIp\": \"2001:0db8:aaaa:bbbb::\"}"), true),
                Arguments.of("cidrContains(/sourceIp,\"2001:0db8::/32\",\"2001:aaaa::/32\")", event("{\"sourceIp\": \"2001:abcd:aaaa:bbbb::\"}"), false)
        );
    }

    private static Stream<Arguments> invalidExpressionArguments() {
        Random random = new Random();

        final String key = RandomStringUtils.randomAlphabetic(5);
        final String value = RandomStringUtils.randomAlphabetic(10);
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event tagEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        String testTag1 = RandomStringUtils.randomAlphabetic(6);
        String testTag2 = RandomStringUtils.randomAlphabetic(7);
        tagEvent.getMetadata().addTags(List.of(testTag1, testTag2));
        String testMetadataKey = RandomStringUtils.randomAlphabetic(5);

        int testStringLength = random.nextInt(10);
        String testString = RandomStringUtils.randomAlphabetic(testStringLength);
        return Stream.of(
                Arguments.of("/missing", event("{}")),
                Arguments.of("/success < /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success <= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success > /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success >= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success > null", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/success >= null", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/status_code < null", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("/status_code <= null", event("{\"success\": true, \"status_code\": 200}")),
                Arguments.of("not /status_code", event("{\"status_code\": 200}")),
                Arguments.of("/status_code >= 200 and 3", event("{\"status_code\": 200}")),
                Arguments.of("", event("{}")),
                Arguments.of("-false", event("{}")),
                Arguments.of("not 5", event("{}")),
                Arguments.of("not null", event("{}")),
                Arguments.of("not/status_code", event("{\"status_code\": 200}")),
                Arguments.of("trueand/status_code", event("{\"status_code\": 200}")),
                Arguments.of("trueor/status_code", event("{\"status_code\": 200}")),
                Arguments.of("length(\""+testString+") == "+testStringLength, event("{\"response\": \""+testString+"\"}")),
                Arguments.of("length(\""+testString+"\") == "+testStringLength, event("{\"response\": \""+testString+"\"}")),
                Arguments.of("hasTags(10)", tagEvent),
                Arguments.of("hasTags("+ testTag1+")", tagEvent),
                Arguments.of("hasTags(\""+ testTag1+")", tagEvent),
                Arguments.of("hasTags(\""+ testTag1+"\","+testTag2+"\")", tagEvent),
                Arguments.of("hasTags(,\""+testTag2+"\")", tagEvent),
                Arguments.of("hasTags(\""+testTag2+"\",)", tagEvent),
                Arguments.of("contains(\""+testTag2+"\",)", tagEvent),
                Arguments.of("contains(\""+testTag2+"\")", tagEvent),
                Arguments.of("contains(/intField, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                Arguments.of("contains(1234, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                Arguments.of("contains(str, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                Arguments.of("contains(/strField, 1234)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                Arguments.of("getMetadata(10)", tagEvent),
                Arguments.of("getMetadata("+ testMetadataKey+ ")", tagEvent),
                Arguments.of("getMetadata(\""+ testMetadataKey+")", tagEvent),
                Arguments.of("cidrContains(/sourceIp)", event("{\"sourceIp\": \"192.0.2.3\"}")),
                Arguments.of("cidrContains(/sourceIp,123)", event("{\"sourceIp\": \"192.0.2.3\"}"))
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
