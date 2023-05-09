/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.matcher.MapEquals.isEqualWithoutTimestamp;

public class JacksonEventTest {
    
    class TestEventHandle implements EventHandle {
        @Override
        public void release(boolean result) {}
    };

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

    @ParameterizedTest
    @ValueSource(strings = {"foo", "foo-bar", "foo_bar", "foo.bar", "/foo", "/foo/", "a1K.k3-01_02"})
    void testPutAndGet_withStrings(final String key) {
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
    }

    @Test
    public void testPutAndGet_withMultLevelKey() {
        final String key = "foo/bar";
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
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
    public void testPutAndGet_withMultiLevelKeyWithADash() {
        final String key = "foo/bar-bar";
        final UUID value = UUID.randomUUID();

        event.put(key, value);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(equalTo(value)));
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

        event.put(key, Arrays.asList(value));

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

    @Test
    public void testDeletingKey() {
        final String key = "foo";

        event.put(key, UUID.randomUUID());
        event.delete(key);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testDelete_withNestedKey() {
        final String key = "foo/bar";

        event.put(key, UUID.randomUUID());
        event.delete(key);
        final UUID result = event.get(key, UUID.class);

        assertThat(result, is(nullValue()));
    }

    @Test
    public void testDelete_withNonexistentKey() {
        final String key = "foo/bar";
        UUID result = event.get(key, UUID.class);
        assertThat(result, is(nullValue()));

        event.delete(key);

        result = event.get(key, UUID.class);
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testContainsKey_withKey() {
        final String key = "foo";

        event.put(key, UUID.randomUUID());
        assertThat(event.containsKey(key), is(true));
    }

    @Test
    public void testContainsKey_withouthKey() {
        final String key = "foo";

        event.put(key, UUID.randomUUID());
        assertThat(event.containsKey("bar"), is(false));
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
    @ValueSource(strings = {"", "withSpecialChars*$%", "-withPrefixDash", "\\-withEscapeChars", "\\\\/withMultipleEscapeChars",
            "withDashSuffix-", "withDashSuffix-/nestedKey", "withDashPrefix/-nestedKey", "_withUnderscorePrefix", "withUnderscoreSuffix_",
            ".withDotPrefix", "withDotSuffix.", "with,Comma", "with:Colon", "with[Bracket", "with|Brace"})
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
        assertThrows(expectedThrowable, () -> event.delete(key));
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
    }

    @Test
    public void testBuild_withTimeReceived() {

        final Instant now = Instant.now();

        event = JacksonEvent.builder()
                .withEventType(eventType)
                .withTimeReceived(now)
                .build();

        assertThat(event.getMetadata().getTimeReceived(), is(equalTo(now)));
    }

    @Test
    public void testBuild_withMessageValue() {

        String message = UUID.randomUUID().toString();

        event = JacksonEvent.fromMessage(message);

        assertThat(event, is(notNullValue()));
        assertThat(event.get("message", String.class), is(equalTo(message)));
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
        final Map<String, Object> testAttributes = new HashMap<>();
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID());
        testAttributes.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String emEventType = UUID.randomUUID().toString();

        final EventMetadata metadata = DefaultEventMetadata.builder()
                .withEventType(emEventType)
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
        assertThat(event.formatString("test-${boo}-string"), is(equalTo(null)));
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
    void testEventHandleGetAndSet() {
        EventHandle testEventHandle = new TestEventHandle();
        final String jsonString = "{\"foo\": \"bar\"}";

        final JacksonEvent event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .build();
        event.setEventHandle(testEventHandle);
        assertThat(event.getEventHandle(), equalTo(testEventHandle));
    }

    @Test
    void testJsonStringBuilder() {
        final String jsonString = "{\"foo\":\"bar\"}";

        final JacksonEvent event = JacksonEvent.builder()
                .withEventType(eventType)
                .withData(jsonString)
                .build();
        final EventMetadata eventMetadata = event.getMetadata();
        eventMetadata.addTag("tag1");
        eventMetadata.addTag("tag2");
        final String expectedJsonString = "{\"foo\":\"bar\",\"tags\":[\"tag1\",\"tag2\"]}";
        assertThat(event.jsonBuilder().includeTags("tags").toJsonString(), equalTo(expectedJsonString));
        assertThat(event.jsonBuilder().toJsonString(), equalTo(jsonString));
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

}
