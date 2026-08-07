/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

class AwsCredentialsSupplierProviderTest {

    @Test
    void getInstance_returnsSameSingleton() {
        assertThat(AwsCredentialsSupplierProvider.getInstance(),
                sameInstance(AwsCredentialsSupplierProvider.getInstance()));
    }

    @Test
    void get_beforeSet_isNull() {
        // fresh instance via the protected test ctor to avoid cross-test static leakage
        final AwsCredentialsSupplierProvider provider = new AwsCredentialsSupplierProvider() {};
        assertThat(provider.getAwsCredentialsSupplier(), nullValue());
    }

    @Test
    void set_thenGet_returnsSuppliedInstance() {
        final AwsCredentialsSupplierProvider provider = new AwsCredentialsSupplierProvider() {};
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        provider.set(supplier);
        assertThat(provider.getAwsCredentialsSupplier(), sameInstance(supplier));
    }

    @Test
    void set_overwritesPrevious() {
        final AwsCredentialsSupplierProvider provider = new AwsCredentialsSupplierProvider() {};
        final AwsCredentialsSupplier first = mock(AwsCredentialsSupplier.class);
        final AwsCredentialsSupplier second = mock(AwsCredentialsSupplier.class);
        provider.set(first);
        provider.set(second);
        assertThat(provider.getAwsCredentialsSupplier(), sameInstance(second));
    }
}
