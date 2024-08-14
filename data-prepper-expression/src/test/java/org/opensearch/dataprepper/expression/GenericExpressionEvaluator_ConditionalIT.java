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
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
                arguments("true", event("{}"), true),
                arguments("/status_code == 200", event("{\"status_code\": 200}"), true),
                arguments("/status_code == 200", longEvent, true),
                arguments("/status_code != 300", event("{\"status_code\": 200}"), true),
                arguments("/status_code == 200", event("{}"), false),
                arguments("/success == /status_code", event("{\"success\": true, \"status_code\": 200}"), false),
                arguments("/success != /status_code", event("{\"success\": true, \"status_code\": 200}"), true),
                arguments("/part1@part2.part3 != 111", event("{\"success\": true, \"part1@part2.part3\":111, \"status_code\": 200}"), false),
                arguments("/part1.part2@part3 != 111", event("{\"success\": true, \"part1.part2@part3\":222, \"status_code\": 200}"), true),
                arguments("/pi == 3.14159", event("{\"pi\": 3.14159}"), true),
                arguments("/value == 12345.678", event("{\"value\": 12345.678}"), true),
                arguments("/value == 12345.678E12", event("{\"value\": 12345.678E12}"), true),
                arguments("/value == 12345.678e-12", event("{\"value\": 12345.678e-12}"), true),
                arguments("/value == 12345.0000012", event("{\"value\": 12345.0000012}"), true),
                arguments("/value == 12345.00012E6", event("{\"value\": 12345.00012E6}"), true),
                arguments("true == (/is_cool == true)", event("{\"is_cool\": true}"), true),
                arguments("not /is_cool", event("{\"is_cool\": true}"), false),
                arguments("/status_code < 300", event("{\"status_code\": 200}"), true),
                arguments("/status_code != null", event("{\"status_code\": 200}"), true),
                arguments("null != /status_code", event("{\"status_code\": 200}"), true),
                arguments("/status_code == null", event("{\"status_code\": null}"), true),
                arguments("/response == null", event("{\"status_code\": 200}"), true),
                arguments("null == /response", event("{\"status_code\": 200}"), true),
                arguments("/response != null", event("{\"status_code\": 200}"), false),
                arguments("/status_code <= 0", event("{\"status_code\": 200}"), false),
                arguments("/status_code > 0", event("{\"status_code\": 200}"), true),
                arguments("/status_code >= 300", event("{\"status_code\": 200}"), false),
                arguments("-/status_code == -200", event("{\"status_code\": 200}"), true),
                arguments("/success and /status_code == 200", event("{\"success\": true, \"status_code\": 200}"), true),
                arguments("/success or /status_code == 200", event("{\"success\": false, \"status_code\": 200}"), true),
                arguments("(/success == true) or (/status_code == 200)", event("{\"success\": false, \"status_code\": 200}"), true),
                arguments("/should_drop", event("{\"should_drop\": true}"), true),
                arguments("/should_drop", event("{\"should_drop\": false}"), false),
                arguments("/logs/2/should_drop", event("{\"logs\": [{}, {}, {\"should_drop\": true}]}"), true),
                arguments(
                        escapedJsonPointer(ALL_JACKSON_EVENT_GET_SUPPORTED_CHARACTERS) + " == true",
                        complexEvent(ALL_JACKSON_EVENT_GET_SUPPORTED_CHARACTERS, true),
                        true),
                arguments("/durationInNanos > 5000000000", event("{\"durationInNanos\": 6000000000}"), true),
                arguments("/response == \"OK\"", event("{\"response\": \"OK\"}"), true),
                arguments("length(/response) == "+testStringLength, event("{\"response\": \""+testString+"\"}"), true),
                arguments("hasTags(\""+ testTag1+"\")", longEvent, true),
                arguments("hasTags(\""+ testTag1+"\",\""+testTag2+"\")", longEvent, true),
                arguments("hasTags(\""+ testTag1+"\", \""+testTag2+"\", \""+testTag3+"\")", longEvent, true),
                arguments("hasTags(\""+ testTag4+"\")", longEvent, false),
                arguments("hasTags(\""+ testTag3+"\",\""+testTag4+"\")", longEvent, false),
                arguments("contains(\""+ strValue+"\",\""+strValue.substring(1,5)+"\")", longEvent, true),
                arguments("contains(/status,\""+strValue.substring(0,2)+"\")", event("{\"status\":\""+strValue+"\"}"), true),
                arguments("contains(\""+strValue+strValue+"\",/status)", event("{\"status\":\""+strValue+"\"}"), true),
                arguments("contains(/message,/status)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), true),
                arguments("contains(/unknown,/status)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), false),
                arguments("contains(/status,/unknown)", event("{\"status\":\""+strValue+"\", \"message\":\""+strValue+strValue+"\"}"), false),
                arguments("getMetadata(\"key1\") == \""+strValue+"\"", longEvent, true),
                arguments("getMetadata(\"key2\") == "+value4, longEvent, true),
                arguments("getMetadata(\"key3\") == "+value5, longEvent, true),
                arguments("getMetadata(\"/key1\") == \""+strValue+"\"", longEvent, true),
                arguments("getMetadata(\"/key2\") == "+value4, longEvent, true),
                arguments("getMetadata(\"key3\") == "+value5, longEvent, true),
                arguments("getMetadata(\"/key6\") == \""+value5+"\"", longEvent, false),
                arguments("getMetadata(\"key6\") == "+value5, longEvent, false),
                arguments("cidrContains(/sourceIp,\"192.0.2.0/24\")", event("{\"sourceIp\": \"192.0.2.3\"}"), true),
                arguments("cidrContains(/sourceIp,\"192.0.2.0/24\",\"192.1.1.0/24\")", event("{\"sourceIp\": \"192.0.2.3\"}"), true),
                arguments("cidrContains(/sourceIp,\"192.0.2.0/24\",\"192.1.1.0/24\")", event("{\"sourceIp\": \"192.2.2.3\"}"), false),
                arguments("cidrContains(/sourceIp,\"2001:0db8::/32\")", event("{\"sourceIp\": \"2001:0db8:aaaa:bbbb::\"}"), true),
                arguments("cidrContains(/sourceIp,\"2001:0db8::/32\",\"2001:aaaa::/32\")", event("{\"sourceIp\": \"2001:0db8:aaaa:bbbb::\"}"), true),
                arguments("cidrContains(/sourceIp,\"2001:0db8::/32\",\"2001:aaaa::/32\")", event("{\"sourceIp\": \"2001:abcd:aaaa:bbbb::\"}"), false),
                arguments("/sourceIp != null", event("{\"sourceIp\": [10, 20]}"), true),
                arguments("/sourceIp == null", event("{\"sourceIp\": [\"test\", \"test_two\"]}"), false),
                arguments("/sourceIp == null", event("{\"sourceIp\": {\"test\": \"test_two\"}}"), false),
                arguments("/sourceIp != null", event("{\"sourceIp\": {\"test\": \"test_two\"}}"), true),
                arguments("/value in {200.222, 300.333, 400}", event("{\"value\": 400}"), true),
                arguments("/value in {200.222, 300.333, 400}", event("{\"value\": 400.222}"), false),
                arguments("/value not in {200.222, 300.333, 400}", event("{\"value\": 400}"), false),
                arguments("/value not in {200.222, 300.333, 400}", event("{\"value\": 800.222}"), true),
                arguments("/color in {\"blue\", \"red\", \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}"), true),
                arguments("/color in {\"blue\", \"red\", \"yellow\", \"green\"}", event("{\"color\": \"gray\"}"), false),
                arguments("/color not in {\"blue\", \"red\", \"yellow\", \"green\"}", event("{\"color\": \"gray\"}"), true),
                arguments("/color not in {\"blue\", \"red\", \"yellow\", \"green\"}", event("{\"color\": \"blue\"}"), false),
                arguments("/color in {\"blue\", \"\", \"red\", \"yellow\", \"green\"}", event("{\"color\": \"\"}"), true),
                arguments("/status_code in {200 , 300}", event("{\"status_code\": 200}"), true),
                arguments("/status_code in {2 , 3}", event("{\"status_code\": 2}"), true),
                arguments("/status_code not in {200 , 300}", event("{\"status_code\": 400}"), true),
                arguments("/status_code in {200 , 300}", event("{\"status_code\": 500}"), false),
                arguments("/flag in {true , false}", event("{\"flag\": false}"), true),
                arguments("/flag in {true , false}", event("{\"flag\": true}"), true),
                arguments("/name =~ \".*dataprepper-[0-9]+\"", event("{\"name\": \"dataprepper-0\"}"), true),
                arguments("/name =~ \".*dataprepper-[0-9]+\"", event("{\"name\": \"dataprepper-212\"}"), true),
                arguments("/name =~ \".*dataprepper-[0-9]+\"", event("{\"name\": \"dataprepper-abc\"}"), false),
                arguments("/name =~ \".*dataprepper-[0-9]+\"", event("{\"other\": \"dataprepper-abc\"}"), false)
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
                arguments("/missing", event("{}")),
                arguments("/success < /status_code", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/success <= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/success > /status_code", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/success >= /status_code", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/success > null", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/success >= null", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/status_code < null", event("{\"success\": true, \"status_code\": 200}")),
                arguments("/status_code <= null", event("{\"success\": true, \"status_code\": 200}")),
                arguments("not /status_code", event("{\"status_code\": 200}")),
                arguments("/status_code >= 200 and 3", event("{\"status_code\": 200}")),
                arguments("", event("{}")),
                arguments("-false", event("{}")),
                arguments("not 5", event("{}")),
                arguments("not null", event("{}")),
                arguments("not/status_code", event("{\"status_code\": 200}")),
                arguments("trueand/status_code", event("{\"status_code\": 200}")),
                arguments("trueor/status_code", event("{\"status_code\": 200}")),
                arguments("length(\""+testString+") == "+testStringLength, event("{\"response\": \""+testString+"\"}")),
                arguments("length(\""+testString+"\") == "+testStringLength, event("{\"response\": \""+testString+"\"}")),
                arguments("hasTags(10)", tagEvent),
                arguments("hasTags("+ testTag1+")", tagEvent),
                arguments("hasTags(\""+ testTag1+")", tagEvent),
                arguments("hasTags(\""+ testTag1+"\","+testTag2+"\")", tagEvent),
                arguments("hasTags(,\""+testTag2+"\")", tagEvent),
                arguments("hasTags(\""+testTag2+"\",)", tagEvent),
                arguments("contains(\""+testTag2+"\",)", tagEvent),
                arguments("contains(\""+testTag2+"\")", tagEvent),
                arguments("contains(/intField, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                arguments("contains(1234, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                arguments("contains(str, /strField)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                arguments("contains(/strField, 1234)", event("{\"intField\":1234,\"strField\":\"string\"}")),
                arguments("/color in {\"blue\", 222.0, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {\"blue, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {\"blue\", yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {\", \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in { \", \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in { , \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {blue, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in { blue, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/color in {\"\",blue, \"yellow\", \"green\"}", event("{\"color\": \"yellow\"}")),
                arguments("/value in {22a2.0, 100}", event("{\"value\": 100}")),
                arguments("/value in {222, 10a0}", event("{\"value\": 100}")),
                arguments("getMetadata(10)", tagEvent),
                arguments("getMetadata("+ testMetadataKey+ ")", tagEvent),
                arguments("getMetadata(\""+ testMetadataKey+")", tagEvent),
                arguments("cidrContains(/sourceIp)", event("{\"sourceIp\": \"192.0.2.3\"}")),
                arguments("cidrContains(/sourceIp,123)", event("{\"sourceIp\": \"192.0.2.3\"}"))
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
