/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultAwsCredentialsSupplierTest {
    @Mock
    private CredentialsProviderFactory credentialsProviderFactory;
    @Mock
    private CredentialsCache credentialsCache;

    private DefaultAwsCredentialsSupplier createObjectUnderTest() {
        return new DefaultAwsCredentialsSupplier(credentialsProviderFactory, credentialsCache);
    }

    @Test
    void getProvider_returns_from_getOrCreate() {
        final AwsCredentialsOptions options = mock(AwsCredentialsOptions.class);

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(credentialsCache.getOrCreate(eq(options), any(Supplier.class)))
                .thenReturn(awsCredentialsProvider);

        assertThat(createObjectUnderTest().getProvider(options), equalTo(awsCredentialsProvider));
    }

    @Test
    void getProvider_calls_getOrCreate_with_Supplier() {
        final AwsCredentialsOptions options = mock(AwsCredentialsOptions.class);
        final ArgumentCaptor<Supplier<AwsCredentialsProvider>> supplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);

        createObjectUnderTest().getProvider(options);

        verify(credentialsCache).getOrCreate(eq(options), supplierArgumentCaptor.capture());
        verifyNoInteractions(credentialsProviderFactory);

        final Supplier<AwsCredentialsProvider> actualCredentialsSupplier = supplierArgumentCaptor.getValue();

        final AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(credentialsProviderFactory.providerFromOptions(options)).thenReturn(awsCredentialsProvider);
        assertThat(actualCredentialsSupplier.get(), equalTo(awsCredentialsProvider));
    }
}