/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ServerSideEncryptionTypeTest {
    @ParameterizedTest
    @EnumSource(ServerSideEncryptionType.class)
    void fromString_returns_expected_value(final ServerSideEncryptionType serverSideEncryptionType) {
        assertThat(ServerSideEncryptionType.fromString(serverSideEncryptionType.toString()), equalTo(serverSideEncryptionType));
    }

    @ParameterizedTest
    @EnumSource(ServerSideEncryptionType.class)
    void toString_returns_non_empty_string_for_all_types(final ServerSideEncryptionType serverSideEncryptionType) {
        assertThat(serverSideEncryptionType.toString(), notNullValue());
        assertThat(serverSideEncryptionType.toString(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(ServerSideEncryptionTypeToKnownName.class)
    void toString_returns_expected_name(final ServerSideEncryptionType serverSideEncryptionType, final String expectedString) {
        assertThat(serverSideEncryptionType.toString(), equalTo(expectedString));
    }

    @ParameterizedTest
    @ArgumentsSource(ServerSideEncryptionTypeToKnownServerSideEncryption.class)
    void getServerSideEncryption_returns_expected_value(final ServerSideEncryptionType serverSideEncryptionType, final ServerSideEncryption expectedServerSideEncryption) {
        assertThat(serverSideEncryptionType.getServerSideEncryption(), equalTo(expectedServerSideEncryption));
    }

    static class ServerSideEncryptionTypeToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(ServerSideEncryptionType.S3, "s3"),
                    arguments(ServerSideEncryptionType.KMS, "kms"),
                    arguments(ServerSideEncryptionType.KMS_DSSE, "kms_dsse")
            );
        }
    }

    static class ServerSideEncryptionTypeToKnownServerSideEncryption implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(ServerSideEncryptionType.S3, ServerSideEncryption.AES256),
                    arguments(ServerSideEncryptionType.KMS, ServerSideEncryption.AWS_KMS),
                    arguments(ServerSideEncryptionType.KMS_DSSE, ServerSideEncryption.AWS_KMS_DSSE)
            );
        }
    }
}
