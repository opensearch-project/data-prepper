/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserAgentProcessorTest {

    private static final int TEST_CACHE_SIZE = 100;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private UserAgentProcessorConfig mockConfig;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @ParameterizedTest
    @MethodSource("userAgentStringArguments")
    public void testParsingUserAgentStrings(
            String uaString, String uaName, String uaVersion, String osName, String osVersion, String osFull, String deviceName) {
        when(mockConfig.getSource()).thenReturn(eventKeyFactory.createEventKey("source"));
        when(mockConfig.getTarget()).thenReturn("user_agent");
        when(mockConfig.getCacheSize()).thenReturn(TEST_CACHE_SIZE);

        final UserAgentProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(uaString);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));
        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.get("user_agent/name", String.class), is(uaName));
        assertThat(resultEvent.get("user_agent/version", String.class), is(uaVersion));
        assertThat(resultEvent.get("user_agent/os/name", String.class), is(osName));
        assertThat(resultEvent.get("user_agent/os/version", String.class), is(osVersion));
        assertThat(resultEvent.get("user_agent/os/full", String.class), is(osFull));
        assertThat(resultEvent.get("user_agent/device/name", String.class), is(deviceName));
        assertThat(resultEvent.get("user_agent/original", String.class), is(uaString));
    }

    @ParameterizedTest
    @MethodSource("userAgentStringArguments")
    public void testParsingUserAgentStringsWithCustomTarget(
            String uaString, String uaName, String uaVersion, String osName, String osVersion, String osFull, String deviceName) {
        when(mockConfig.getSource()).thenReturn(eventKeyFactory.createEventKey("source"));
        when(mockConfig.getTarget()).thenReturn("my_target");
        when(mockConfig.getCacheSize()).thenReturn(TEST_CACHE_SIZE);

        final UserAgentProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(uaString);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));
        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.get("my_target/name", String.class), is(uaName));
        assertThat(resultEvent.get("my_target/version", String.class), is(uaVersion));
        assertThat(resultEvent.get("my_target/os/name", String.class), is(osName));
        assertThat(resultEvent.get("my_target/os/version", String.class), is(osVersion));
        assertThat(resultEvent.get("my_target/os/full", String.class), is(osFull));
        assertThat(resultEvent.get("my_target/device/name", String.class), is(deviceName));
        assertThat(resultEvent.get("my_target/original", String.class), is(uaString));
    }

    @ParameterizedTest
    @MethodSource("userAgentStringArguments")
    public void testParsingUserAgentStringsExcludeOriginal(
            String uaString, String uaName, String uaVersion, String osName, String osVersion, String osFull, String deviceName) {
        when(mockConfig.getSource()).thenReturn(eventKeyFactory.createEventKey("source"));
        when(mockConfig.getTarget()).thenReturn("user_agent");
        when(mockConfig.getExcludeOriginal()).thenReturn(true);
        when(mockConfig.getCacheSize()).thenReturn(TEST_CACHE_SIZE);

        final UserAgentProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(uaString);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));
        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.get("user_agent/name", String.class), is(uaName));
        assertThat(resultEvent.get("user_agent/version", String.class), is(uaVersion));
        assertThat(resultEvent.get("user_agent/os/name", String.class), is(osName));
        assertThat(resultEvent.get("user_agent/os/version", String.class), is(osVersion));
        assertThat(resultEvent.get("user_agent/os/full", String.class), is(osFull));
        assertThat(resultEvent.get("user_agent/device/name", String.class), is(deviceName));
        assertThat(resultEvent.containsKey("user_agent/original"), is(false));
    }

    @Test
    public void testParsingWhenUserAgentStringNotExist() {
        when(mockConfig.getSource()).thenReturn(eventKeyFactory.createEventKey("bad_source"));
        when(mockConfig.getCacheSize()).thenReturn(TEST_CACHE_SIZE);
        when(mockConfig.getTarget()).thenReturn("user_agent");

        final UserAgentProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(UUID.randomUUID().toString());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));
        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.containsKey("user_agent"), is(false));
    }

    @Test
    public void testTagsAddedOnParseFailure() {
        when(mockConfig.getSource()).thenReturn(eventKeyFactory.createEventKey("bad_source"));
        when(mockConfig.getCacheSize()).thenReturn(TEST_CACHE_SIZE);
        when(mockConfig.getTarget()).thenReturn("user_agent");

        final String tagOnFailure1 = UUID.randomUUID().toString();
        final String tagOnFailure2 = UUID.randomUUID().toString();
        when(mockConfig.getTagsOnParseFailure()).thenReturn(List.of(tagOnFailure1, tagOnFailure2));

        final UserAgentProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(UUID.randomUUID().toString());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));
        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.containsKey("user_agent"), is(false));
        assertThat(resultEvent.getMetadata().getTags().contains(tagOnFailure1), is(true));
        assertThat(resultEvent.getMetadata().getTags().contains(tagOnFailure2), is(true));
    }

    private UserAgentProcessor createObjectUnderTest() {
        return new UserAgentProcessor(mockConfig, eventKeyFactory, pluginMetrics);
    }

    private Record<Event> createTestRecord(String uaString) {
        final Map<String, Object> data = Map.of("source", uaString);
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private static Stream<Arguments> userAgentStringArguments() {
        return Stream.of(
                Arguments.of(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                        "Chrome", "51.0.2704", "Linux", "", "Linux", "Other"
                ),
                Arguments.of(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.6; rv:42.0) Gecko/20100101 Firefox/42.0",
                        "Firefox", "42.0", "Mac OS X", "12.6", "Mac OS X 12.6", "Mac"
                ),
                Arguments.of(
                        "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
                        "Mobile Safari", "13.1.1", "iOS", "13.5.1", "iOS 13.5.1", "iPhone"
                ),
                Arguments.of(
                        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                        "Googlebot", "2.1", "Other", "", "Other", "Spider"
                ),
                Arguments.of(
                        "PostmanRuntime/7.26.5",
                        "PostmanRuntime", "7.26.5", "Other", "", "Other", "Other"
                ),
                Arguments.of(
                        UUID.randomUUID().toString(),
                        "Other", "", "Other", "", "Other", "Other"
                )
        );
    }
}