/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.exceptions.EventKeyNotFoundException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.matcher.MapEquals.isEqualWithoutTimestamp;

public class JacksonEventTest {

    private Event event;

    private String eventType;

    @BeforeEach
    public void setup() {

        eventType = UUID.randomUUID().toString();

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .build();

    }

    @Test
    public void testPutAndGet_withRandomString() {
        final String key = "aRandomKey" + UUID.randomUUID();
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withRandomString_eventKey() {
        final EventKey key = new JacksonEventKey("aRandomKey" + UUID.randomUUID());
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withRandomString_eventKey_multiple_events() {
        final EventKey key = new JacksonEventKey("aRandomKey" + UUID.randomUUID());
        final UUID value = UUID.randomUUID();

        for(int i = 0; i < 10; i++) {
            event = JacksonEvent.builder()
                    .withEventType(eventType)
                    .build();

            event.put(key, value);
            final UUID result = event.get(key, UUID.class);

            assertThat(result, is(notNullValue()));
            assertThat(result, is(equalTo(value)));
        }
    }

    @Test
    public void testPutAndGet_eventKey_with_non_JacksonEventKey_throws() {
        final EventKey key = mock(EventKey.class);
        final UUID value = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class, () -> event.put(key, value));
        assertThrows(IllegalArgumentException.class, () -> event.get(key, UUID.class));
    }

    @Test
    public void testPut_eventKey_with_immutable_action() {
        final EventKey key = new JacksonEventKey("aRandomKey" + UUID.randomUUID(), EventKeyFactory.EventAction.GET);
        final UUID value = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class, () -> event.put(key, value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "foo-bar", "foo_bar", "foo.bar", "/foo", "/foo/", "a1K.k3-01_02", "keyWithBrackets[]"})
    void testPutAndGet_withStrings(final String key) {
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "foo-bar", "foo_bar", "foo.bar", "/foo", "/foo/", "a1K.k3-01_02", "keyWithBrackets[]"})
    void testPutAndGet_withStrings_eventKey(final String key) {
        final UUID value = UUID.randomUUID();

        final EventKey eventKey = new JacksonEventKey(key);
        event.put(eventKey, value);
        final UUID result = event.get(eventKey, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    void testPutAndGet_withArrays_eventKey() {

        final String key = "list-key/0/foo";
        final String newValue = UUID.randomUUID().toString();

        final List<Map<String, Object>> listValue = new ArrayList<>();
        final Map<String, Object> mapValue = Map.of("foo", "bar", "foo-2", "bar-2");
        listValue.add(mapValue);

        final String listKey = "list-key";
        final EventKey eventKey = new JacksonEventKey(listKey);
        event.put(eventKey, listValue);

        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put(listKey, listValue);

        assertThat(event.toMap(), equalTo(expectedMap));

        final EventKey eventNestedKey = new JacksonEventKey(key);
        event.put(eventNestedKey, newValue);

        final List<Map<String, Object>> newlistValue = new ArrayList<>();
        final Map<String, Object> newMapValue = Map.of("foo", newValue, "foo-2", "bar-2");
        newlistValue.add(newMapValue);

        expectedMap.put(listKey, newlistValue);

        assertThat(event.toMap(), equalTo(expectedMap));


        final List<Map<String, Object>> result = event.get(listKey, List.class);
        assertThat(result, equalTo(newlistValue));

        final String resultValue = event.get(key, String.class);
        assertThat(resultValue, equalTo(newValue));
    }

    @Test
    void testUpdateFailureMetadata() {
        Object failureMetadata = event.updateFailureMetadata();
        assertThat(failureMetadata, instanceOf(JacksonEvent.DefaultEventFailureMetadata.class));
        assertThat(event.get(JacksonEvent.DefaultEventFailureMetadata.FAILURE_METADATA, Map.class), is(nullValue()));
        ((EventFailureMetadata)failureMetadata).with("key", "value");
        assertThat(event.get(JacksonEvent.DefaultEventFailureMetadata.FAILURE_METADATA, Map.class), is(notNullValue()));
    }

    @Test
    public void testDefaultEventFailureMetadata() {
        String eventType = UUID.randomUUID().toString();

        Event event = JacksonEvent.builder()
                .withEventType(eventType)
                .build();

        EventFailureMetadata eventFailureMetadata = event.updateFailureMetadata();
        eventFailureMetadata.with("key1", "value1").with("key2", 2);
        assertThat(event.get(JacksonEvent.DefaultEventFailureMetadata.FAILURE_METADATA+"/key1", String.class), equalTo("value1"));
        assertThat(event.get(JacksonEvent.DefaultEventFailureMetadata.FAILURE_METADATA+"/key2", Integer.class), equalTo(2));
    }

    @Test
    void testPutAndGet_withArrays_out_of_bounds_on_end_of_list_creates_new_element() {

        final String key = "list-key/1/foo";
        final String fooValue = UUID.randomUUID().toString();

        final List<Map<String, Object>> listValue = new ArrayList<>();
        final Map<String, Object> mapValue = Map.of("foo", "bar", "foo-2", "bar-2");
        listValue.add(mapValue);

        final String listKey = "list-key";
        final EventKey eventKey = new JacksonEventKey(listKey);
        event.put(eventKey, listValue);

        event.put(key, fooValue);

        final String resultValue = event.get(key, String.class);
        assertThat(resultValue, equalTo(fooValue));
    }

    @Test
    void testPutAndGet_withArrays_out_of_bounds_throws_IndexOutOfBoundsException() {

        final String key = "list-key/3/foo";
        final String fooValue = UUID.randomUUID().toString();

        final List<Map<String, Object>> listValue = new ArrayList<>();
        final Map<String, Object> mapValue = Map.of("foo", "bar", "foo-2", "bar-2");
        listValue.add(mapValue);

        final String listKey = "list-key";
        final EventKey eventKey = new JacksonEventKey(listKey);
        event.put(eventKey, listValue);

        assertThrows(IndexOutOfBoundsException.class, () -> event.put(key, fooValue));
    }

    @Test
    public void testPutKeyCannotBeEmptyString() {
        Throwable exception = assertThrows(IllegalArgumentException.class, () -> event.put("", "value"));
        assertThat(exception.getMessage(), containsStringIgnoringCase("key cannot be an empty string"));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "key&1, key_1",
            "key^1, key_1",
            "key%1, key%1",
            "key_1, key_1"
    })
    public void testReplaceInvalidKeyChars(final String key, final String expected) {
        assertThat(JacksonEvent.replaceInvalidKeyChars(key), equalTo(expected));
        assertThat(JacksonEvent.replaceInvalidKeyChars(key.substring(0,3)), equalTo("key"));
        assertThat(JacksonEvent.replaceInvalidKeyChars(null), equalTo(null));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "key&1, key_1",
            "key^1, key_1",
            "key%1, key%1",
            "key_1, key_1"
    })
    public void testPutWithReplaceInvalidKeyChars(final String key, final String expectedKey) {
        final String value = UUID.randomUUID().toString();

        event.put(key, value, true);
        assertThat(event.get(expectedKey, String.class), equalTo(value));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "key&1, key_1",
            "key^1, key_1",
            "key%1, key%1",
            "key_1, key_1"
    })
    public void testPutWithReplaceInvalidKeyChars_for_map(final String key, final String expectedKey) {
        final String value = UUID.randomUUID().toString();
        final Map<String, String> mapToPut = Map.of(key, value);

        event.put("myMap", mapToPut, true);
        assertThat(event.get("myMap/" + expectedKey, String.class), equalTo(value));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "key&1, key_1",
            "key^1, key_1",
            "key%1, key%1",
            "key_1, key_1"
    })
    public void testPutEventKeyWithReplaceInvalidKeyChars_for_map(final String key, final String expectedKey) {
        final String value = UUID.randomUUID().toString();
        final Map<String, String> mapToPut = Map.of(key, value);

        final EventKey eventKey = new JacksonEventKey("myMap");
        event.put(eventKey, mapToPut, true);
        assertThat(event.get("myMap/" + expectedKey, String.class), equalTo(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"key&1", "key^1", "key%1", "key_1"})
    public void testPutEventKeyWithReplaceInvalidKeyCharsFalse_for_map(final String key) {
        final String value = UUID.randomUUID().toString();
        final Map<String, String> mapToPut = Map.of(key, value);

        final EventKey eventKey = new JacksonEventKey("myMap");
        event.put(eventKey, mapToPut, false);
    }

    @ParameterizedTest
    @ValueSource(strings = {"key&1", "key^1"})
    public void testPutWithoutReplaceInvalidKeyChars(final String key) {
        final String value = UUID.randomUUID().toString();

        assertThrows(IllegalArgumentException.class, () -> event.put(key, value, false));
    }

    @Test
    public void testPutAndGet_withMultiLevelKey() {
        final String key = "foo/bar";
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withMultiLevelKey_eventKey() {
        final EventKey key = new JacksonEventKey("foo/bar");
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withMultiLevelInvalidValues() {
        final Map<String, Object> data1 = new HashMap<>();
        final Map<String, Object> data2 = new HashMap<>();
        final Map<String, Object> data3 = new HashMap<>();
        data3.put("key^5", "value5");
        data2.put("key^3", 3);
        data2.put("key%4", data3);
        data1.put("key&2", data2);

        event.put("foo", data1, true);
        assertThat(event.get("foo/key_2/key_3", Integer.class), equalTo(3));
        assertThat(event.get("foo/key_2/key%4/key_5", String.class), equalTo("value5"));
    }

    @Test
    public void testPutAndGet_withMultiLevelInvalidValues_eventKey() {
        final EventKey key = new JacksonEventKey("foo");
        final Map<String, Object> data1 = new HashMap<>();
        final Map<String, Object> data2 = new HashMap<>();
        final Map<String, Object> data3 = new HashMap<>();
        data3.put("key^5", "value5");
        data2.put("key^3", 3);
        data2.put("key%4", data3);
        data1.put("key&2", data2);

        event.put(key, data1, true);
        assertThat(event.get("foo/key_2/key_3", Integer.class), equalTo(3));
        assertThat(event.get("foo/key_2/key%4/key_5", String.class), equalTo("value5"));
    }

    @Test
    public void testPutAndGet_withMultiLevelKeyTwice() {
        final String key = "foo/bar";
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));

        final String key2 = "foo/fizz";
        final UUID value2 = UUID.randomUUID();

        event.put(key2, value2);
        final UUID result2 = event.get(key2, UUID.class);

        assertThat(result2, is(notNullValue()));
        assertThat(result2, is(equalTo(value2)));
    }

    @Test
    public void testPutAndGet_withMultiLevelKeyTwice_eventKey() {
        final EventKey key = new JacksonEventKey("foo/bar");
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));

        final EventKey key2 = new JacksonEventKey("foo/fizz");
        final UUID value2 = UUID.randomUUID();

        event.put(key2, value2);
        final UUID result2 = event.get(key2, UUID.class);

        assertThat(result2, is(notNullValue()));
        assertThat(result2, is(equalTo(value2)));
    }

    @Test
    public void testPutAndGet_withMultiLevelKeyWithADash() {
        final String key = "foo/bar-bar";
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withMultiLevelKeyWithADash_eventKey() {
        final EventKey key = new JacksonEventKey("foo/bar-bar");
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo", "/foo", "/foo/", "foo/"})
    void testGetAtRootLevel(final String key) {
        final String value = UUID.randomUUID().toString();

        event.put(key, value);
        final Map<String, String> result = event.get("", Map.class);

        assertThat(result, is(Map.of("foo", value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo", "/foo", "/foo/", "foo/"})
    void testGetAtRootLevel_eventKey(final String key) {
        final String value = UUID.randomUUID().toString();

        event.put(new JacksonEventKey(key), value);
        final Map<String, String> result = event.get(new JacksonEventKey("", EventKeyFactory.EventAction.GET), Map.class);

        assertThat(result, is(Map.of("foo", value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/foo/bar", "foo/bar", "foo/bar/"})
    void testGetAtRootLevelWithMultiLevelKey(final String key) {
        final String value = UUID.randomUUID().toString();

        event.put(key, value);
        final Map<String, String> result = event.get("", Map.class);

        assertThat(result, is(Map.of("foo", Map.of("bar", value))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/foo/bar", "foo/bar", "foo/bar/"})
    void testGetAtRootLevelWithMultiLevelKey_eventKey(final String key) {
        final String value = UUID.randomUUID().toString();

        event.put(new JacksonEventKey(key), value);
        final Map<String, String> result = event.get( new JacksonEventKey("", EventKeyFactory.EventAction.GET), Map.class);

        assertThat(result, is(Map.of("foo", Map.of("bar", value))));
    }

    @Test
    public void testPutUpdateAndGet_withPojo() {
        final String key = "foo/bar";
        final String nestedValue = UUID.randomUUID().toString();
        final TestObject value = new TestObject(nestedValue);
        final String nestedKey = "foo/bar/field1";

        event.put(key, value);
        final String actualNestedValue = event.get(nestedKey, String.class);

        assertThat(actualNestedValue, is(notNullValue()));
        assertThat(actualNestedValue, is(equalTo(nestedValue)));

        final String replacementValue = UUID.randomUUID().toString();
        event.put(nestedKey, replacementValue);
        final TestObject result = event.get(key, TestObject.class);

        assertThat(result, is(notNullValue()));
        assertThat(result.getField1(), is(equalTo(replacementValue)));
    }

    @Test
    public void testPutUpdateAndGet_withList() {
        final String key = "foo/bar";
        final List<Integer> numbers = Arrays.asList(1, 2, 3);

        event.put(key, numbers);

        final List<Integer> value = event.get(key, List.class);
        assertThat(value, is(notNullValue()));
        assertThat(value, is(is(equalTo(numbers))));

        final String item2Key = key + "/1";
        event.put(item2Key, 42);

        final Integer item2Value = event.get(item2Key, Integer.class);
        assertThat(item2Value, is(notNullValue()));
        assertThat(item2Value, is(equalTo(42)));
    }

    @Test
    public void testGet_withIncorrectPojo() {
        final String key = "foo/bar";
        final String nestedValue = UUID.randomUUID().toString();
        final TestObject value = new TestObject(nestedValue);

        event.put(key, value);

        assertThrows(RuntimeException.class, () -> event.get(key, UUID.class));
    }

    @Test
    public void testGetList_withIncorrectPojo() {
        final String key = "foo.bar";
        final String nestedValue = UUID.randomUUID().toString();
        final TestObject value = new TestObject(nestedValue);

        event.put(key, List.of(value));

        assertThrows(RuntimeException.class, () -> event.getList(key, UUID.class));
    }

    @Test
    public void testGetList_Missing() {
        assertThat(event.getList("missingKey", Object.class), is(nullValue()));
    }

    @Test
    public void testGet_withEmptyEvent() {
        final String key = "foo/bar";

        UUID result = event.get(key, UUID.class);

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testGetToJsonString_nestedObject() {
        final String key = "foo/bar";
        final String nestedValue = UUID.randomUUID().toString();
        final TestObject value = new TestObject(nestedValue);

        event.put(key, value);
        final String actualNestedValue = event.getAsJsonString(key);

        assertThat(actualNestedValue, is(equalTo(String.format("{\"field1\":\"%s\"}", nestedValue))));
    }

    @Test
    public void testGetToJsonString_randomKeys() {
        final String key = "aRandomKey" + UUID.randomUUID();
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final String result = event.getAsJsonString(key);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(String.format("\"%s\"", value))));
    }

    @Test
    public void testGetToJsonString_keysThatDoNotExist() {
        final String key = "aRandomKey" + UUID.randomUUID();
        final UUID value = UUID.randomUUID();

        event.put("staticKey", value);
        final String result = event.getAsJsonString(key);

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testOverwritingExistingKey() {
        final String key = "foo/bar";
        final UUID value = UUID.randomUUID();

        event.put(key, UUID.randomUUID());
        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "/foo", "/foo/bar", "foo/bar", "foo/bar/", "/foo/bar/leaf/key", "keyWithBrackets[]"})
    public void testDeleteKey(final String key) {
        event.put(key, UUID.randomUUID());
        event.delete(key);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testDelete_eventKey_with_immutable_action() {
        final EventKey key = new JacksonEventKey("aRandomKey" + UUID.randomUUID(), EventKeyFactory.EventAction.GET);
        final UUID value = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class, () -> event.delete(key));
    }

    @Test
    public void testClear() {
        event.put("key1", UUID.randomUUID());
        event.put("key2", UUID.randomUUID());
        event.put("key3/key4", UUID.randomUUID());
        event.clear();
        UUID result = event.get("key1", UUID.class);
        assertThat(result, is(nullValue()));
        result = event.get("key2", UUID.class);
        assertThat(result, is(nullValue()));
        result = event.get("key3", UUID.class);
        assertThat(result, is(nullValue()));
        result = event.get("key3/key4", UUID.class);
        assertThat(result, is(nullValue()));
        assertThat(event.toMap().size(), equalTo(0));
    }

    @Test
    void merge_with_non_JacksonEvent_throws() {
        final Event otherEvent = mock(Event.class);
        assertThrows(IllegalArgumentException.class, () -> event.merge(otherEvent));
    }

    @Test
    void merge_with_array_JsonNode_throws() {
        final JacksonEvent otherEvent = (JacksonEvent) event;
        event = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(List.of(UUID.randomUUID().toString())).build();
        assertThrows(UnsupportedOperationException.class, () -> event.merge(otherEvent));
    }

    @Test
    void merge_with_array_JsonNode_in_other_throws() {
        Event otherEvent = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(List.of(UUID.randomUUID().toString())).build();
        assertThrows(IllegalArgumentException.class, () -> event.merge(otherEvent));
    }

    @Test
    void merge_sets_all_values() {
        final String jsonString = "{\"a\": \"alpha\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        event.put("b", "original");
        Event otherEvent = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(jsonString).build();
        event.merge(otherEvent);

        assertThat(event.get("b", Object.class), equalTo("original"));
        assertThat(event.get("a", Object.class), equalTo("alpha"));
        assertThat(event.containsKey("info"), equalTo(true));
        assertThat(event.get("info/ids/id", String.class), equalTo("idx"));
    }

    @Test
    void merge_overrides_existing_values() {
        final String jsonString = "{\"a\": \"alpha\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        event.put("a", "original");
        event.put("b", "original");
        Event otherEvent = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(jsonString).build();
        event.merge(otherEvent);

        assertThat(event.get("b", Object.class), equalTo("original"));
        assertThat(event.get("a", Object.class), equalTo("alpha"));
        assertThat(event.containsKey("info"), equalTo(true));
        assertThat(event.get("info/ids/id", String.class), equalTo("idx"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "/foo", "/foo/bar", "foo/bar", "foo/bar/", "/foo/bar/leaf/key"})
    public void testDelete_withNonexistentKey(final String key) {
        UUID result = event.get(key, UUID.class);
        assertThat(result, is(nullValue()));

        event.delete(key);

        result = event.get(key, UUID.class);
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testDeleteKeyCannotBeEmptyString() {
        Throwable exception = assertThrows(IllegalArgumentException.class, () -> event.delete(""));
        assertThat(exception.getMessage(), containsStringIgnoringCase("key cannot be an empty string"));
    }

    @Test
    public void testContainsKeyReturnsTrueForEmptyStringKey() {
        assertThat(event.containsKey(""), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "/foo", "/foo/bar", "foo/bar", "foo/bar/", "/foo/bar/leaf/key"})
    public void testContainsKey_withKey(final String key) {
        event.put(key, UUID.randomUUID());
        assertThat(event.containsKey(key), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "foo", "/foo", "/foo/bar", "foo/bar", "foo/bar/", "/foo/bar/leaf/key"})
    public void testContainsKey_withouthKey(final String key) {
        assertThat(event.containsKey(key), is(false));
    }

    @Test
    public void testIsValueAList_withAList() {
        final String key = "foo";
        final List<Integer> numbers = Arrays.asList(1, 2, 3);

        event.put(key, numbers);
        assertThat(event.isValueAList(key), is(true));
    }

    @Test
    public void testIsValueAList_withoutAList() {
        final String key = "foo";
        event.put(key, UUID.randomUUID());
        assertThat(event.isValueAList(key), is(false));
    }

    @Test
    public void testIsValueAList_withNull() {
        final String key = "foo";
        event.put(key, null);
        assertThat(event.isValueAList(key), is(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"withSpecialChars*$%", "\\-withEscapeChars", "\\\\/withMultipleEscapeChars",
            "with,Comma", "with|Brace"})
    void testKey_withInvalidKey_throwsIllegalArgumentException(final String invalidKey) {
        assertThrowsForKeyCheck(IllegalArgumentException.class, invalidKey);
    }

    @Test
    public void testKey_withLengthGreaterThanMaxLength_throwsIllegalArgumentException() {
        final String invalidLengthKey = RandomStringUtils.random(JacksonEvent.MAX_KEY_LENGTH + 1);
        assertThrowsForKeyCheck(IllegalArgumentException.class, invalidLengthKey);
    }

    @Test
    public void testKey_withNullKey_throwsNullPointerException() {
        assertThrowsForKeyCheck(NullPointerException.class, null);
    }

    private <T extends Throwable> void assertThrowsForKeyCheck(final Class<T> expectedThrowable, final String key) {
        assertThrows(expectedThrowable, () -> event.put(key, UUID.randomUUID()));
        assertThrows(expectedThrowable, () -> event.get(key, String.class));
    }

    @Test
    public void testToString_withEmptyData() {
        final String result = event.toJsonString();

        assertThat(result, is(equalTo("{}")));
    }

    @Test
    public void testToString_withSimpleObject() {
        event.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        event.put("testObject", new TestObject(value));
        event.put("list", Arrays.asList(1, 4, 5));
        final String result = event.toJsonString();

        assertThat(result, is(equalTo(String.format("{\"foo\":\"bar\",\"testObject\":{\"field1\":\"%s\"},\"list\":[1,4,5]}", value))));
    }

    @Test
    public void testGetAsMap_with_EmptyData() {
        final Map<String, Object> eventAsMap = event.toMap();
        assertThat(eventAsMap, isEqualWithoutTimestamp(Collections.emptyMap()));
    }

    @Test
    public void testGetAsMap_withSimpleEvent() {
        final Map<String, Object> mapObject = new HashMap<>();

        event.put("foo", "bar");
        mapObject.put("foo", "bar");

        event.put("list", Arrays.asList(1, 4, 5));
        mapObject.put("list", Arrays.asList(1, 4, 5));

        final Map<String, Object> eventAsMap = event.toMap();
        assertThat(eventAsMap, isEqualWithoutTimestamp(mapObject));
    }

    @Test
    public void testBuild_withEventType() {
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .build();

        assertThat(event.getMetadata().getEventType(), is(equalTo(eventType)));
        assertThat(event.getEventHandle(), is(notNullValue()));
        assertThat(event.getEventHandle().getInternalOriginationTime(), is(notNullValue()));
    }

    @Test
    public void testBuild_withEventHandle() {
        final Instant now = Instant.now();

        EventHandle eventHandle = new DefaultEventHandle(now);
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withEventHandle(eventHandle)
                .build();

        assertThat(event.getEventHandle(), is(eventHandle));
        assertThat(event.getEventHandle().getInternalOriginationTime(), is(equalTo(now)));
    }

    @Test
    public void testBuild_withTimeReceived() {

        final Instant now = Instant.now();

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withTimeReceived(now)
                .build();

        assertThat(event.getMetadata().getTimeReceived(), is(equalTo(now)));
        assertThat(event.getEventHandle(), is(notNullValue()));
        assertThat(event.getEventHandle().getInternalOriginationTime(), is(equalTo(now)));
    }

    @Test
    public void testBuild_withMessageValue() {

        String message = UUID.randomUUID().toString();

        event = JacksonEvent.fromMessage(message);

        assertThat(event, is(notNullValue()));
        assertThat(event.get("message", String.class), is(equalTo(message)));
        assertThat(event.getEventHandle(), is(notNullValue()));
        assertThat(event.getEventHandle().getInternalOriginationTime(), is(notNullValue()));
    }

    @Test
    public void testBuild_withAttributes() {

        final Map<String, Object> testAttributes = new HashMap<>();
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID());
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withEventMetadataAttributes(testAttributes)
                .build();

        assertThat(event.getMetadata().getAttributes(), is(equalTo(testAttributes)));
    }

    @Test
    public void testBuild_withAllMetadataFields() {

        final Instant now = Instant.now().minusSeconds(1);
        final Instant extTime = Instant.now().minusSeconds(10);
        final Map<String, Object> testAttributes = new HashMap<>();
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID());
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String emEventType = UUID.randomUUID().toString();

        final EventMetadata metadata = DefaultEventMetadata.builder()
                .withEventType(emEventType)
                .withExternalOriginationTime(extTime)
                .build();

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withTimeReceived(now)
                .withEventMetadataAttributes(testAttributes)
                .withEventMetadata(metadata)
                .build();

        assertThat(event.getMetadata().getAttributes(), is(not(equalTo(testAttributes))));
        assertThat(event.getMetadata().getTimeReceived(), is(not(equalTo(now))));
        assertThat(event.getMetadata().getEventType(), is(equalTo(emEventType)));
        assertThat(event.getMetadata().getExternalOriginationTime(), is(equalTo(extTime)));
        assertThat(event.getEventHandle().getExternalOriginationTime(), is(equalTo(extTime)));
    }

    @Test
    public void testBuild_withEventMetadata() {

        EventMetadata metadata = DefaultEventMetadata.builder()
                .withEventType(eventType)
                .build();

        event = JacksonEvent.builder()
                .withEventMetadata(metadata)
                .build();

        assertThat(event.getMetadata(), is(equalTo(metadata)));
    }


    @Test
    public void testBuild_withData() {

        final String value = UUID.randomUUID().toString();
        final TestObject testObject = new TestObject(value);

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(testObject)
                .getThis()
                .build();

        assertThat(event.get("field1", String.class), is(equalTo(value)));
    }

    @Test
    public void testBuild_withStringData() {

        final String jsonString = "{\"foo\": \"bar\"}";

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        assertThat(event.get("foo", String.class), is(equalTo("bar")));
    }

    @ParameterizedTest
    @CsvSource({
            "test-string, test-string",
            "test-${foo}-string, test-bar-string",
            "test-string-${foo}, test-string-bar",
            "${foo}-test-string, bar-test-string",
            "test-${info/ids/id}-string, test-idx-string",
            "test-string-${info/ids/id}, test-string-idx",
            "${info/ids/id}-test-string, idx-test-string",
            "${info/ids/id}-test-string-${foo}, idx-test-string-bar",
            "${info/ids/id}-test-${foo}-string, idx-test-bar-string",
            "${info/ids/id}-${foo}-test-string, idx-bar-test-string",
            "${info/ids/id}${foo}-test-string, idxbar-test-string",
    })
    public void testBuild_withFormatString(String formattedString, String finalString) {

        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        assertThat(event.formatString(formattedString), is(equalTo(finalString)));
    }

    @Test
    public void formatString_with_expression_evaluator_catches_exception_when_Event_get_throws_exception() {

        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        final String expressionStatement = UUID.randomUUID().toString();
        final String invalidKeyExpression = "getMetadata(\"metadata-key\")";
        final String invalidKeyExpressionResult = UUID.randomUUID().toString();
        final String expressionEvaluationResult = UUID.randomUUID().toString();

        final String formatString = "${" + invalidKeyExpression + "}-${" + expressionStatement + "}-test-string";
        final String finalString = invalidKeyExpressionResult + "-" + expressionEvaluationResult + "-test-string";

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);

        when(expressionEvaluator.isValidExpressionStatement("foo")).thenReturn(false);
        when(expressionEvaluator.isValidExpressionStatement(expressionStatement)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(invalidKeyExpression)).thenReturn(true);
        when(expressionEvaluator.evaluate(invalidKeyExpression, event)).thenReturn(invalidKeyExpressionResult);
        when(expressionEvaluator.evaluate(expressionStatement, event)).thenReturn(expressionEvaluationResult);

        assertThat(event.formatString(formatString, expressionEvaluator), is(equalTo(finalString)));
    }

    @Test
    public void testBuild_withFormatStringWithExpressionEvaluator() {

        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        final String expressionStatement = UUID.randomUUID().toString();
        final String expressionEvaluationResult = UUID.randomUUID().toString();
        final String eventKey = "foo";

        final String formatString = "${" + eventKey + "}-${" + expressionStatement + "}-test-string";
        final String finalString = "bar-" + expressionEvaluationResult + "-test-string";

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);

        verify(expressionEvaluator, times(0)).isValidExpressionStatement(eventKey);
        when(expressionEvaluator.isValidExpressionStatement(expressionStatement)).thenReturn(true);
        verify(expressionEvaluator, never()).evaluate(eq("foo"), any(Event.class));
        when(expressionEvaluator.evaluate(expressionStatement, event)).thenReturn(expressionEvaluationResult);

        assertThat(event.formatString(formatString, expressionEvaluator), is(equalTo(finalString)));
    }

    @ParameterizedTest
    @CsvSource({
            "test-${foo}-string, test-123-string",
            "${info/ids/id}-${foo}-test-string, true-123-test-string",
    })
    public void testBuild_withFormatStringWithIntegerBoolean(String formattedString, String finalString) {

        final String jsonString = "{\"foo\": 123, \"info\": {\"ids\": {\"id\":true}}}";

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();
        assertThat(event.formatString(formattedString), is(equalTo(finalString)));
    }

    @Test
    public void testBuild_withFormatStringWithValueNotFound() {

        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();
        assertThrows(EventKeyNotFoundException.class, () -> event.formatString("test-${boo}-string"));
    }

    @Test
    public void testBuild_withFormatStringWithValueNotFound_and_defaultValue_for_missing_keys() {

        final String defaultValueForMissingKey = UUID.randomUUID().toString();
        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();
        final String result = event.formatString("test-${boo}-string", expressionEvaluator, defaultValueForMissingKey);
        assertThat(result, equalTo("test-" + defaultValueForMissingKey + "-string"));
    }

    @Test
    public void testBuild_withFormatStringWithInvalidFormat() {

        final String jsonString = "{\"foo\": \"bar\", \"info\": {\"ids\": {\"id\":\"idx\"}}}";
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();
        assertThrows(RuntimeException.class, () -> event.formatString("test-${foo-string"));

    }

    @Test
    public void testBuild_withInvalidStringData() {

        final String jsonString = "foobar";

        final JacksonEvent.Builder builder = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis();

        assertThrows(IllegalArgumentException.class, () -> builder.build());
    }

    @Test
    void fromEvent_with_a_JacksonEvent() {
        final Map<String, Object> dataObject = createComplexDataMap();

        final JacksonEvent originalEvent = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(dataObject)
                .build();

        final JacksonEvent createdEvent = JacksonEvent.fromEvent(originalEvent);

        assertThat(createdEvent, notNullValue());
        assertThat(createdEvent, not(sameInstance(originalEvent)));
        assertThat(event.getEventHandle(), is(notNullValue()));
        assertThat(event.getEventHandle(), instanceOf(DefaultEventHandle.class));
        assertThat(event.getEventHandle().getInternalOriginationTime(), is(notNullValue()));

        assertThat(createdEvent.toMap(), equalTo(dataObject));
        assertThat(createdEvent.getJsonNode(), not(sameInstance(originalEvent.getJsonNode())));

        assertThat(createdEvent.getMetadata(), notNullValue());
        assertThat(createdEvent.getMetadata(), not(sameInstance(originalEvent.getMetadata())));
        assertThat(createdEvent.getMetadata(), equalTo(originalEvent.getMetadata()));
    }

    @Test
    void fromEvent_with_a_non_JacksonEvent() {
        final Map<String, Object> dataObject = createComplexDataMap();

        final EventMetadata eventMetadata = mock(EventMetadata.class);
        final Event originalEvent = mock(Event.class);
        when(originalEvent.toMap()).thenReturn(dataObject);
        when(originalEvent.getMetadata()).thenReturn(eventMetadata);

        final JacksonEvent createdEvent = JacksonEvent.fromEvent(originalEvent);

        assertThat(createdEvent, notNullValue());
        assertThat(createdEvent, not(sameInstance(originalEvent)));

        assertThat(createdEvent.toMap(), equalTo(dataObject));

        assertThat(createdEvent.getMetadata(), notNullValue());
        assertThat(createdEvent.getMetadata(), equalTo(eventMetadata));
    }

    @Test
    void testJsonStringBuilder() {
        final String jsonString = "{\"foo\":\"bar\"}";

        final JacksonEvent event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .build();
        final EventMetadata eventMetadata = event.getMetadata();
        eventMetadata.addTags(List.of("tag1", "tag2"));
        final String expectedJsonString = "{\"foo\":\"bar\",\"tags\":[\"tag1\",\"tag2\"]}";
        assertThat(event.jsonBuilder().includeTags("tags").toJsonString(), equalTo(expectedJsonString));
        assertThat(event.jsonBuilder().rootKey("foo").toJsonString(), equalTo("\"bar\""));
        assertThat(event.jsonBuilder().toJsonString(), equalTo(jsonString));
    }

    @Test
    void testJsonStringBuilderWithIncludeKeys() {
        final String jsonString = "{\"id\":1,\"foo\":\"bar\",\"info\":{\"name\":\"hello\",\"foo\":\"bar\"},\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        // Include Keys must start with / and also ordered, This is pre-processed in SinkModel
        List<String> includeNullKey = null;
        assertThat(event.jsonBuilder().rootKey(null).includeKeys(includeNullKey).toJsonString(), equalTo(jsonString));

        List<String> includeEmptyKey = List.of();
        assertThat(event.jsonBuilder().rootKey(null).includeKeys(includeEmptyKey).toJsonString(), equalTo(jsonString));

        // Include Keys must start with / and also ordered, This is pre-processed in SinkModel
        List<String> includeKeys1 = Arrays.asList("foo", "info");
        final String expectedJsonString1 = "{\"foo\":\"bar\",\"info\":{\"name\":\"hello\",\"foo\":\"bar\"}}";
        assertThat(event.jsonBuilder().rootKey(null).includeKeys(includeKeys1).toJsonString(), equalTo(expectedJsonString1));

        // Test child node
        List<String> includeKeys2 = Arrays.asList("foo", "info/name");
        final String expectedJsonString2 = "{\"foo\":\"bar\",\"info\":{\"name\":\"hello\"}}";
        assertThat(event.jsonBuilder().includeKeys(includeKeys2).toJsonString(), equalTo(expectedJsonString2));

        // Test array node.
        List<String> includeKeys3 = Arrays.asList("foo", "tags/key");
        final String expectedJsonString3 = "{\"foo\":\"bar\",\"tags\":[{\"key\":\"a\"},{\"key\":\"c\"}]}";
        assertThat(event.jsonBuilder().includeKeys(includeKeys3).toJsonString(), equalTo(expectedJsonString3));

        // Test some keys not found
        List<String> includeKeys4 = Arrays.asList("foo", "info/age");
        final String expectedJsonString4 = "{\"foo\":\"bar\",\"info\":{}}";
        assertThat(event.jsonBuilder().includeKeys(includeKeys4).toJsonString(), equalTo(expectedJsonString4));

        // Test all keys not found
        List<String> includeKeys5 = List.of("/ello");
        final String expectedJsonString5 = "{}";
        assertThat(event.jsonBuilder().includeKeys(includeKeys5).toJsonString(), equalTo(expectedJsonString5));

        // Test working with root node
        List<String> includeKeys6 = List.of("name");
        final String expectedJsonString6 = "{\"name\":\"hello\"}";
        assertThat(event.jsonBuilder().rootKey("info").includeKeys(includeKeys6).toJsonString(), equalTo(expectedJsonString6));

        // Test working with unknown root node
        List<String> includeKeys7 = List.of("name");
        final String expectedJsonString7 = "{}";
        assertThat(event.jsonBuilder().rootKey("hello").includeKeys(includeKeys7).toJsonString(), equalTo(expectedJsonString7));


    }

    @Test
    void testJsonStringBuilderWithExcludeKeys() {
        final String jsonString = "{\"id\":1,\"foo\":\"bar\",\"info\":{\"name\":\"hello\",\"foo\":\"bar\"},\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .getThis()
                .build();

        // Include Keys must start with / and also ordered, This is pre-processed in SinkModel
        List<String> excludeKeys1 = Arrays.asList("foo", "info");
        final String expectedJsonString1 = "{\"id\":1,\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        assertThat(event.jsonBuilder().rootKey(null).excludeKeys(excludeKeys1).toJsonString(), equalTo(expectedJsonString1));

        // Test child node
        List<String> excludeKeys2 = Arrays.asList("foo", "info/name");
        final String expectedJsonString2 = "{\"id\":1,\"info\":{\"foo\":\"bar\"},\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        assertThat(event.jsonBuilder().excludeKeys(excludeKeys2).toJsonString(), equalTo(expectedJsonString2));

        // Test array node.
        List<String> excludeKeys3 = Arrays.asList("foo", "tags/key");
        final String expectedJsonString3 = "{\"id\":1,\"info\":{\"name\":\"hello\",\"foo\":\"bar\"},\"tags\":[{\"value\":\"b\"},{\"value\":\"d\"}]}";
        assertThat(event.jsonBuilder().excludeKeys(excludeKeys3).toJsonString(), equalTo(expectedJsonString3));

        // Test some keys not found
        List<String> excludeKeys4 = Arrays.asList("foo", "info/age");
        final String expectedJsonString4 = "{\"id\":1,\"info\":{\"name\":\"hello\",\"foo\":\"bar\"},\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        assertThat(event.jsonBuilder().excludeKeys(excludeKeys4).toJsonString(), equalTo(expectedJsonString4));

        // Test all keys not found
        List<String> excludeKeys5 = List.of("hello");
        final String expectedJsonString5 = "{\"id\":1,\"foo\":\"bar\",\"info\":{\"name\":\"hello\",\"foo\":\"bar\"},\"tags\":[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]}";
        assertThat(event.jsonBuilder().excludeKeys(excludeKeys5).toJsonString(), equalTo(expectedJsonString5));

        // Test working with root node
        List<String> excludeKeys6 = List.of("name");
        final String expectedJsonString6 = "{\"foo\":\"bar\"}";
        assertThat(event.jsonBuilder().rootKey("info").excludeKeys(excludeKeys6).toJsonString(), equalTo(expectedJsonString6));

        // Test working with unknown root node
        List<String> includeKeys7 = List.of("name");
        final String expectedJsonString7 = "{}";
        assertThat(event.jsonBuilder().rootKey("hello").includeKeys(includeKeys7).toJsonString(), equalTo(expectedJsonString7));

    }

    @ParameterizedTest
    @CsvSource(value = {"test_key, true",
            "/test_key, true",
            "inv(alid, true",
            "getMetadata(\"test_key\"), false",
            "key.with.dot, true",
            "key-with-hyphen, true",
            "key_with_underscore, true",
            "key@with@at, true",
            "key[with]brackets, true",
            "key%with%percent, true",
            "key:with:colon, true"
    })
    void isValidEventKey_returns_expected_result(final String key, final boolean isValid) {
        assertThat(JacksonEvent.isValidEventKey(key), equalTo(isValid));
    }

    private static Map<String, Object> createComplexDataMap() {
        final Map<String, Object> dataObject = new HashMap<>();
        final int fullDepth = 6;
        final Random random = new Random();
        Map<String, Object> currentObject = dataObject;
        for (int depth = 0; depth < fullDepth; depth++) {
            currentObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            currentObject.put(UUID.randomUUID().toString(), random.nextInt(10_000) + 1000);

            final Map<String, Object> nextObject = new HashMap<>();
            currentObject.put(UUID.randomUUID().toString(), nextObject);
            currentObject = nextObject;
        }
        return dataObject;
    }

    @ParameterizedTest
    @MethodSource("getBigDecimalPutTestData")
    void testPutAndGet_withBigDecimal(final String value) {
        final String key = "bigDecimalKey";
        event.put(key, new BigDecimal(value));
        final Object result = event.get(key, Object.class);
        assertThat(result, is(notNullValue()));
        assertThat(result.toString(), is(equalTo(value)));
    }

    private static Stream<Arguments> getBigDecimalPutTestData() {
        return Stream.of(
                Arguments.of("702062202420"),
                Arguments.of("1.23345E+9"),
                Arguments.of("1.2345E+60"),
                Arguments.of("1.2345E+6"),
                Arguments.of("1.000")
        );
    }
}
