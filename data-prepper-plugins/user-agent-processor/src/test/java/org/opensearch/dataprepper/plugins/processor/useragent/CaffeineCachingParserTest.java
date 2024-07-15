/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ua_parser.Client;
import ua_parser.Device;
import ua_parser.OS;
import ua_parser.UserAgent;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("StringOperationCanBeSimplified")
class CaffeineCachingParserTest {
    private static final String KNOWN_USER_AGENT_STRING = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1";
    long cacheSize;

    @BeforeEach
    void setUp() {
        cacheSize = 1000;
    }

    private CaffeineCachingParser createObjectUnderTest() {
        return new CaffeineCachingParser(cacheSize);
    }

    @Test
    void parse_returns_expected_results() {
        final Client client = createObjectUnderTest().parse(KNOWN_USER_AGENT_STRING);

        assertThat(client, notNullValue());
        assertThat(client.userAgent, notNullValue());
        assertThat(client.userAgent.family, equalTo("Mobile Safari"));
        assertThat(client.userAgent.major, equalTo("13"));
        assertThat(client.device.family, equalTo("iPhone"));
        assertThat(client.os.family, equalTo("iOS"));
    }

    @Test
    void parse_with_null_returns_null() {
        assertThat(createObjectUnderTest().parse(null),
                nullValue());
    }

    @Test
    void parse_called_multiple_times_returns_same_instance() {
        final CaffeineCachingParser objectUnderTest = createObjectUnderTest();

        final String userAgentString = KNOWN_USER_AGENT_STRING;
        final Client client = objectUnderTest.parse(userAgentString);

        assertThat(client, notNullValue());

        assertThat(objectUnderTest.parse(new String(userAgentString)), sameInstance(client));
        assertThat(objectUnderTest.parse(new String(userAgentString)), sameInstance(client));
        assertThat(objectUnderTest.parse(new String(userAgentString)), sameInstance(client));
    }

    @Test
    void parseUserAgent_returns_expected_results() {
        final UserAgent userAgent = createObjectUnderTest().parseUserAgent(KNOWN_USER_AGENT_STRING);

        assertThat(userAgent, notNullValue());
        assertThat(userAgent.family, equalTo("Mobile Safari"));
        assertThat(userAgent.major, equalTo("13"));
    }

    @Test
    void parseUserAgent_with_null_returns_null() {
        assertThat(createObjectUnderTest().parseUserAgent(null),
                nullValue());
    }

    @Test
    void parseUserAgent_called_multiple_times_returns_same_instance() {
        final CaffeineCachingParser objectUnderTest = createObjectUnderTest();

        final String userAgentString = KNOWN_USER_AGENT_STRING;
        final UserAgent userAgent = objectUnderTest.parseUserAgent(userAgentString);

        assertThat(userAgent, notNullValue());

        assertThat(objectUnderTest.parseUserAgent(new String(userAgentString)), sameInstance(userAgent));
        assertThat(objectUnderTest.parseUserAgent(new String(userAgentString)), sameInstance(userAgent));
        assertThat(objectUnderTest.parseUserAgent(new String(userAgentString)), sameInstance(userAgent));
    }

    @Test
    void parseDevice_returns_expected_results() {
        final Device device = createObjectUnderTest().parseDevice(KNOWN_USER_AGENT_STRING);

        assertThat(device, notNullValue());
        assertThat(device.family, equalTo("iPhone"));
    }

    @Test
    void parseDevice_with_null_returns_null() {
        assertThat(createObjectUnderTest().parseDevice(null),
                nullValue());
    }

    @Test
    void parseDevice_called_multiple_times_returns_same_instance() {
        final CaffeineCachingParser objectUnderTest = createObjectUnderTest();

        final String userAgentString = KNOWN_USER_AGENT_STRING;
        final Device device = objectUnderTest.parseDevice(userAgentString);

        assertThat(device, notNullValue());

        assertThat(objectUnderTest.parseDevice(new String(userAgentString)), sameInstance(device));
        assertThat(objectUnderTest.parseDevice(new String(userAgentString)), sameInstance(device));
        assertThat(objectUnderTest.parseDevice(new String(userAgentString)), sameInstance(device));
    }

    @Test
    void parseOS_returns_expected_results() {
        final OS os = createObjectUnderTest().parseOS(KNOWN_USER_AGENT_STRING);

        assertThat(os, notNullValue());
        assertThat(os.family, equalTo("iOS"));
        assertThat(os.major, equalTo("13"));
    }

    @Test
    void parseOS_with_null_returns_null() {
        assertThat(createObjectUnderTest().parseOS(null),
                nullValue());
    }

    @Test
    void parseOS_called_multiple_times_returns_same_instance() {
        final CaffeineCachingParser objectUnderTest = createObjectUnderTest();

        final String userAgentString = KNOWN_USER_AGENT_STRING;
        final OS os = objectUnderTest.parseOS(userAgentString);

        assertThat(os, notNullValue());

        assertThat(objectUnderTest.parseOS(new String(userAgentString)), sameInstance(os));
        assertThat(objectUnderTest.parseOS(new String(userAgentString)), sameInstance(os));
        assertThat(objectUnderTest.parseOS(new String(userAgentString)), sameInstance(os));
    }
}