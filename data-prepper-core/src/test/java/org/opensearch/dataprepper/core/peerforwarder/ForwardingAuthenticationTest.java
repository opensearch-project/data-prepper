/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.core.peerforwarder.ForwardingAuthentication;

import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ForwardingAuthenticationTest {

    @ParameterizedTest
    @ArgumentsSource(EnumToStringNameArgumentsProvider.class)
    void getValue_returns_expected_value (final ForwardingAuthentication enumValue, final String expectedName) {
        assertThat(enumValue.getName(), equalTo(expectedName));
    }

    @ParameterizedTest
    @EnumSource(ForwardingAuthentication.class)
    void getByName_returns_correct_enum_from_expected_name(final ForwardingAuthentication enumValue) {

        final String stringName = enumValue.getName();

        assertThat(ForwardingAuthentication.getByName(stringName), equalTo(enumValue));
    }

    @Test
    void getByName_throws_for_null() {
        assertThrows(IllegalArgumentException.class, ()  -> ForwardingAuthentication.getByName(null));
    }

    @Test
    void getByName_throws_for_empty_string() {
        assertThrows(IllegalArgumentException.class, ()  -> ForwardingAuthentication.getByName(""));
    }

    @Test
    void getByName_throws_for_unrecognized_non_empty_name() {
        assertThrows(IllegalArgumentException.class, ()  -> ForwardingAuthentication.getByName(UUID.randomUUID().toString()));
    }

    private static class EnumToStringNameArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(ForwardingAuthentication.MUTUAL_TLS, "mutual_tls"),
                    arguments(ForwardingAuthentication.UNAUTHENTICATED, "unauthenticated")
            );
        }
    }
}