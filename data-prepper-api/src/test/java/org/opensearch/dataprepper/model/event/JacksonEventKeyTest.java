/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.core.JsonPointer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JacksonEventKeyTest {
    @Test
    void constructor_throws_with_null_key() {
        assertThrows(NullPointerException.class, () -> new JacksonEventKey(null));
    }

    @Test
    void getKey_with_empty_string_for_GET() {
        final JacksonEventKey objectUnderTest = new JacksonEventKey("", EventKeyFactory.EventAction.GET);
        assertThat(objectUnderTest.getKey(), equalTo(""));
        assertThat(objectUnderTest.getTrimmedKey(), equalTo(""));
        assertThat(objectUnderTest.getKeyPathList(), notNullValue());
        assertThat(objectUnderTest.getKeyPathList(), equalTo(List.of("")));
        assertThat(objectUnderTest.getJsonPointer(), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class, mode = EnumSource.Mode.EXCLUDE, names = {"GET"})
    void constructor_throws_with_empty_string_for_unsupported_actions(final EventKeyFactory.EventAction eventAction) {
        assertThrows(IllegalArgumentException.class, () -> new JacksonEventKey("", eventAction));
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "inv&alid",
            "getMetadata(\"test_key\")"
    })
    void constructor_throws_with_invalid_key(final String key) {
        assertThrows(IllegalArgumentException.class, () -> new JacksonEventKey(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "test_key",
            "/test_key",
            "key.with.dot",
            "key-with-hyphen",
            "key_with_underscore",
            "key@with@at",
            "key[with]brackets",
            "key~1withtilda",
            "key with space",
            "key$with$dollar",
            " key_with_space_prefix",
            "key_with_space_suffix ",
            "$key_with_dollar_prefix",
            "key_with_dollar_suffix$"
    })
    void getKey_returns_expected_result(final String key) {
        assertThat(new JacksonEventKey(key).getKey(), equalTo(key));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "test_key, test_key",
            "/test_key, /test_key",
            "/test_key/, /test_key",
            "key.with.dot, key.with.dot",
            "key-with-hyphen, key-with-hyphen",
            "key_with_underscore, key_with_underscore",
            "key@with@at, key@with@at",
            "key[with]brackets, key[with]brackets"
    })
    void getTrimmedKey_returns_expected_result(final String key, final String expectedTrimmedKey) {
        assertThat(new JacksonEventKey(key).getTrimmedKey(), equalTo(expectedTrimmedKey));
    }

    @ParameterizedTest
    @ArgumentsSource(KeyPathListArgumentsProvider.class)
    void getKeyPathList_returns_expected_value(final String key, final List<String> expectedKeyPathList) {
        assertThat(new JacksonEventKey(key).getKeyPathList(), equalTo(expectedKeyPathList));
    }

    @Test
    void getJsonPointer_returns_a_valid_JsonPointer() {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey);

        final JsonPointer jsonPointer = objectUnderTest.getJsonPointer();
        assertThat(jsonPointer, notNullValue());
        assertThat(jsonPointer.toString(), equalTo("/" + testKey));
    }

    @Test
    void getJsonPointer_returns_the_same_instance_for_multiple_calls() {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey);

        final JsonPointer jsonPointer = objectUnderTest.getJsonPointer();
        assertThat(objectUnderTest.getJsonPointer(), sameInstance(jsonPointer));
        assertThat(objectUnderTest.getJsonPointer(), sameInstance(jsonPointer));
    }

    @ParameterizedTest
    @EnumSource(value = EventKeyFactory.EventAction.class)
    void getJsonPointer_returns_valid_JsonPointer_when_constructed_with_fromJacksonEvent(final EventKeyFactory.EventAction eventAction) {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, true, eventAction);

        final JsonPointer jsonPointer = objectUnderTest.getJsonPointer();
        assertThat(jsonPointer, notNullValue());
        assertThat(jsonPointer.toString(), equalTo("/" + testKey));
    }

    @ParameterizedTest
    @ArgumentsSource(KeyPathListArgumentsProvider.class)
    void getKeyPathList_returns_expected_value_when_constructed_with_fromJacksonEvent(final String key, final List<String> expectedKeyPathList) {
        assertThat(new JacksonEventKey(key, true).getKeyPathList(), equalTo(expectedKeyPathList));
    }

    @ParameterizedTest
    @ArgumentsSource(SupportsArgumentsProvider.class)
    void supports_returns_true_if_any_supports(final List<EventKeyFactory.EventAction> eventActionsList, final EventKeyFactory.EventAction otherAction, final boolean expectedSupports) {
        final String testKey = UUID.randomUUID().toString();
        final EventKeyFactory.EventAction[] eventActions = new EventKeyFactory.EventAction[eventActionsList.size()];
        eventActionsList.toArray(eventActions);
        assertThat(new JacksonEventKey(testKey, eventActions).supports(otherAction), equalTo(expectedSupports));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "test_key, true",
            "/test_key, true",
            "inv(alid, true",
            "getMetadata(\"test_key\"), false",
            "key.with.dot, true",
            "key-with-hyphen, true",
            "key_with_underscore, true",
            "key@with@at, true",
            "key[with]brackets, true",
            "key with space, true",
            "key$with$dollar, true",
            " key_with_space_prefix, true",
            "key_with_space_suffix , true",
            "$key_with_dollar_prefix, true",
            "key_with_dollar_suffix$, true",
            "key%with%percent, true",
            "key:with:colon, true"
    })
    void isValidEventKey_returns_expected_result(final String key, final boolean isValid) {
        assertThat(JacksonEventKey.isValidEventKey(key), equalTo(isValid));
    }


    static class KeyPathListArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments("test_key", List.of("test_key")),
                    arguments("a/b", List.of("a", "b")),
                    arguments("a/b/", List.of("a", "b")),
                    arguments("a/b/c", List.of("a", "b", "c")),
                    arguments("a/b/c/", List.of("a", "b", "c"))
            );
        }
    }

    static class SupportsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments(List.of(), EventKeyFactory.EventAction.GET, true),
                    arguments(List.of(), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(), EventKeyFactory.EventAction.DELETE, true),
                    arguments(List.of(), EventKeyFactory.EventAction.ALL, true),
                    arguments(List.of(EventKeyFactory.EventAction.GET), EventKeyFactory.EventAction.GET, true),
                    arguments(List.of(EventKeyFactory.EventAction.PUT), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(EventKeyFactory.EventAction.DELETE), EventKeyFactory.EventAction.DELETE, true),
                    arguments(List.of(EventKeyFactory.EventAction.GET), EventKeyFactory.EventAction.PUT, false),
                    arguments(List.of(EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.PUT), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.GET), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(EventKeyFactory.EventAction.DELETE), EventKeyFactory.EventAction.PUT, false),
                    arguments(List.of(EventKeyFactory.EventAction.DELETE, EventKeyFactory.EventAction.GET), EventKeyFactory.EventAction.PUT, false),
                    arguments(List.of(EventKeyFactory.EventAction.DELETE, EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.PUT), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(EventKeyFactory.EventAction.ALL), EventKeyFactory.EventAction.GET, true),
                    arguments(List.of(EventKeyFactory.EventAction.ALL), EventKeyFactory.EventAction.PUT, true),
                    arguments(List.of(EventKeyFactory.EventAction.ALL), EventKeyFactory.EventAction.DELETE, true)
            );
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "key&1, key_1",
            "key^1, key_1",
            "key%1, key%1",
            "key_1, key_1"
    })
    public void testReplaceInvalidKeyChars(final String key, final String expected) {
        assertThat(JacksonEventKey.replaceInvalidCharacters(key), equalTo(expected));
        assertThat(JacksonEventKey.replaceInvalidCharacters(null), equalTo(null));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void equals_returns_true_for_same_key_and_actions(final EventKeyFactory.EventAction eventAction) {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, eventAction);
        final JacksonEventKey other = new JacksonEventKey(testKey, eventAction);

        assertThat(objectUnderTest.equals(other), equalTo(true));
    }

    @Test
    void equals_returns_true_for_same_instance() {
        final JacksonEventKey objectUnderTest = new JacksonEventKey(UUID.randomUUID().toString(),
                EventKeyFactory.EventAction.PUT);

        assertThat(objectUnderTest.equals(objectUnderTest), equalTo(true));
    }

    @Test
    void equals_returns_false_for_null() {
        final JacksonEventKey objectUnderTest = new JacksonEventKey(UUID.randomUUID().toString(),
                EventKeyFactory.EventAction.PUT);

        assertThat(objectUnderTest.equals(null), equalTo(false));
    }

    @Test
    void equals_returns_false_for_non_EventKey() {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey,
                EventKeyFactory.EventAction.PUT);

        assertThat(objectUnderTest.equals(testKey), equalTo(false));
    }

    @Test
    void equals_returns_false_for_same_key_but_different_actions() {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, EventKeyFactory.EventAction.PUT);
        final JacksonEventKey other = new JacksonEventKey(testKey, EventKeyFactory.EventAction.GET);

        assertThat(objectUnderTest.equals(other), equalTo(false));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void equals_returns_false_for_different_key_but_same_actions(final EventKeyFactory.EventAction eventAction) {
        final JacksonEventKey objectUnderTest = new JacksonEventKey(UUID.randomUUID().toString(), eventAction);
        final JacksonEventKey other = new JacksonEventKey(UUID.randomUUID().toString(), eventAction);

        assertThat(objectUnderTest.equals(other), equalTo(false));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void hashCode_is_the_same_for_same_key_and_actions(final EventKeyFactory.EventAction eventAction) {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, eventAction);
        final JacksonEventKey other = new JacksonEventKey(testKey, eventAction);

        assertThat(objectUnderTest.hashCode(), equalTo(other.hashCode()));
    }

    @ParameterizedTest
    @CsvSource({
            "test, PUT, test2, PUT",
            "test, PUT, test2, PUT",
            "test, PUT, test, GET"
    })
    void hashCode_is_the_different_for_same_key_and_actions(
            final String testKey, final EventKeyFactory.EventAction eventAction,
            final String testKeyOther, final EventKeyFactory.EventAction eventActionOther) {
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, eventAction);
        final JacksonEventKey other = new JacksonEventKey(testKeyOther, eventActionOther);

        assertThat(objectUnderTest.hashCode(), not(equalTo(other.hashCode())));
    }

    @ParameterizedTest
    @EnumSource(EventKeyFactory.EventAction.class)
    void toString_returns_the_key(final EventKeyFactory.EventAction eventAction) {
        final String testKey = UUID.randomUUID().toString();
        final JacksonEventKey objectUnderTest = new JacksonEventKey(testKey, eventAction);

        assertThat(objectUnderTest.toString(), equalTo(testKey));
    }
}
