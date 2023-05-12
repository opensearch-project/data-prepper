/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class CidrExpressionFunctionTest {
    private CidrExpressionFunction cidrExpressionFunction;
    private Event testEvent;

    @Mock
    private Function<Object, Object> testFunction;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private CidrExpressionFunction createObjectUnderTest() {
        return new CidrExpressionFunction();
    }

    @BeforeEach
    public void setUp() {
        cidrExpressionFunction = createObjectUnderTest();
    }

    @Test
    void testIpv4MatchWithSingleCidrBlock() {
        String network = "\"192.0.2.0/24\"";
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.2.3"));
        Object expressionResult = cidrExpressionFunction.evaluate(List.of("/sourceIp", network), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(true));
    }

    @Test
    void testIpv4MatchWithMultipleCidrBlocks() {
        String network1 = "\"192.0.2.0/24\"";
        String network2 = "\"192.168.1.0/24\"";
        String network3 = "\"10.0.1.0/24\"";
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.2.3"));
        Object expressionResult = cidrExpressionFunction.evaluate(
                List.of("/sourceIp", network1, network2, network3), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(true));
    }

    @Test
    void testIpv4NonMatchWithMultipleCidrBlocks() {
        String network1 = "\"192.0.2.0/24\"";
        String network2 = "\"192.168.1.0/24\"";
        String network3 = "\"10.0.1.0/24\"";
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.5.3"));
        Object expressionResult = cidrExpressionFunction.evaluate(
                List.of("/sourceIp", network1, network2, network3), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(false));
    }

    @Test
    void testIpv6MatchWithSingleCidrBlock() {
        String network = "\"2001:0db8::/32\"";
        testEvent = createTestEvent(Map.of("sourceIp", "2001:0db8:aaaa:bbbb::"));
        Object expressionResult = cidrExpressionFunction.evaluate(List.of("/sourceIp", network), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(true));
    }

    @Test
    void testIpv6MatchWithMultipleCidrBlocks() {
        String network1 = "\"2001:0db8::/32\"";
        String network2 = "\"2001:aaaa::/32\"";
        String network3 = "\"2001:bbbb:cccc::/48\"";
        testEvent = createTestEvent(Map.of("sourceIp", "2001:0db8:aaaa:bbbb::"));
        Object expressionResult = cidrExpressionFunction.evaluate(List.of("/sourceIp", network1, network2, network3), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(true));
    }

    @Test
    void testIpv6NonMatchWithMultipleCidrBlocks() {
        String network1 = "\"2001:0db8::/32\"";
        String network2 = "\"2001:aaaa::/32\"";
        String network3 = "\"2001:bbbb:cccc::/48\"";
        testEvent = createTestEvent(Map.of("sourceIp", "2001:dddd:aaaa:bbbb::"));
        Object expressionResult = cidrExpressionFunction.evaluate(List.of("/sourceIp", network1, network2, network3), testEvent, testFunction);
        assertThat((boolean)expressionResult, equalTo(false));
    }

    @Test
    void testTooFewArgumentsThrowsException() {
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.2.3"));
        assertThrows(IllegalArgumentException.class,
                () -> cidrExpressionFunction.evaluate(List.of("/sourceIp"), testEvent, testFunction));
    }

    @Test
    void testArgumentTypeNotSupportedThrowsException() {
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.2.3"));
        assertThrows(IllegalArgumentException.class,
                () -> cidrExpressionFunction.evaluate(List.of("/sourceIp", 123), testEvent, testFunction));
    }

    @Test
    void testIpAddressNotExistInEventThrowsException() {
        String network = "\"192.0.2.0/24\"";
        testEvent = createTestEvent(Map.of("sourceIp", "192.0.2.3"));
        assertThrows(NullPointerException.class,
                () -> cidrExpressionFunction.evaluate(List.of("/destinationIp", network), testEvent, testFunction));
    }
}
