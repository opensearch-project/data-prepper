package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.hamcrest.MatcherAssert.assertThat;

public class IsolationLevelTest {

    @ParameterizedTest
    @EnumSource(IsolationLevel.class)
    void fromTypeValue_should_return_expected_enum(final IsolationLevel value) {
        assertThat(IsolationLevel.fromTypeValue(value.getType()), is(value));
        assertThat(value, instanceOf(IsolationLevel.class));
    }

    @ParameterizedTest
    @ArgumentsSource(IsolationLevelToKnownName.class)
    void fromTypeValue_returns_expected_value(final IsolationLevel isolationLevel, final String knownString) {
        assertThat(IsolationLevel.fromTypeValue(knownString), equalTo(isolationLevel));
    }

    @ParameterizedTest
    @EnumSource(IsolationLevel.class)
    void getType_returns_non_empty_string_for_all_types(final IsolationLevel isolationLevel) {
        assertThat(isolationLevel.getType(), notNullValue());
        assertThat(isolationLevel.getType(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(IsolationLevelToKnownName.class)
    void getType_returns_expected_string(final IsolationLevel isolationLevel, final String expectedString) {
        assertThat(isolationLevel.getType(), equalTo(expectedString));
    }

    @Test
    void fromTypeValue_returns_null_for_unknown_string() {
        assertThat(IsolationLevel.fromTypeValue("unknown"), nullValue());
        assertThat(IsolationLevel.fromTypeValue("READ_COMMITED_WRONG_CASE"), nullValue());
    }

    static class IsolationLevelToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(IsolationLevel.READ_UNCOMMITTED, "read_uncommitted"),
                    arguments(IsolationLevel.READ_COMMITTED, "read_committed")
            );
        }
    }
}
