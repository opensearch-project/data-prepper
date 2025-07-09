/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialsCacheTest {
    @Mock
    private AwsCredentialsOptions credentialsOptions;

    @Mock
    private Supplier<AwsCredentialsProvider> credentialsProviderSupplier;

    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @BeforeEach
    void setUp() {
        when(credentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(credentialsProviderSupplier.get()).thenReturn(credentialsProvider);
    }

    private CredentialsCache createObjectUnderTest() {
        return new CredentialsCache();
    }

    @Test
    void getOrCreate_with_single_object_returns_expected_value() {
        final CredentialsCache objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getOrCreate(credentialsOptions, credentialsProviderSupplier),
                equalTo(credentialsProvider));

        assertThat(objectUnderTest.getOrCreate(credentialsOptions, credentialsProviderSupplier),
                equalTo(credentialsProvider));

        verify(credentialsProviderSupplier).get();
    }

    @Test
    void getOrCreate_returns_value_from_supplier_as_expected_with_multiple_objects() {
        final AwsCredentialsOptions existingCredentialsOptions = mock(AwsCredentialsOptions.class);
        when(existingCredentialsOptions.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        final Supplier<AwsCredentialsProvider> existingCredentialsProviderSupplier = mock(Supplier.class);
        final AwsCredentialsProvider existingCredentialsProvider = mock(AwsCredentialsProvider.class);

        when(existingCredentialsProviderSupplier.get()).thenReturn(existingCredentialsProvider);

        final CredentialsCache objectUnderTest = createObjectUnderTest();
        objectUnderTest.getOrCreate(existingCredentialsOptions, existingCredentialsProviderSupplier);

        assertThat(objectUnderTest.getOrCreate(credentialsOptions, credentialsProviderSupplier),
                equalTo(credentialsProvider));

        assertThat(objectUnderTest.getOrCreate(credentialsOptions, credentialsProviderSupplier),
                equalTo(credentialsProvider));

        assertThat(objectUnderTest.getOrCreate(existingCredentialsOptions, existingCredentialsProviderSupplier),
                equalTo(existingCredentialsProvider));

        verify(existingCredentialsProviderSupplier).get();
        verify(credentialsProviderSupplier).get();
    }
}
