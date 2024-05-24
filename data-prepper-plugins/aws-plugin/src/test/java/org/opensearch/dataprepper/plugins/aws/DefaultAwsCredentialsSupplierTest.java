/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Optional;
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

    @ParameterizedTest
    @MethodSource("getRegions")
    void getDefaultRegion_returns_default_region(final Region region) {
        when(credentialsProviderFactory.getDefaultRegion()).thenReturn(region);

        final AwsCredentialsSupplier objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getDefaultRegion().isPresent(), equalTo(true));
        assertThat(objectUnderTest.getDefaultRegion().get(), equalTo(credentialsProviderFactory.getDefaultRegion()));
    }

    @Test
    void no_default_region_returns_empty_optional() {
        when(credentialsProviderFactory.getDefaultRegion()).thenReturn(null);

        final AwsCredentialsSupplier objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getDefaultRegion(), equalTo(Optional.empty()));

    }

    private static List<Region> getRegions() {
        return Region.regions();
    }
}