/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class GcpExtensionProviderTest {
    @Mock
    private GcpCredentialsSupplier gcpCredentialsSupplier;

    @Mock
    private ExtensionProvider.Context context;

    @Test
    void provideInstance_returns_supplier_and_supportedClass_is_supplier_type() {
        final GcpExtensionProvider objectUnderTest = new GcpExtensionProvider(gcpCredentialsSupplier);

        assertThat(objectUnderTest.provideInstance(context), equalTo(Optional.of(gcpCredentialsSupplier)));
        assertThat(objectUnderTest.supportedClass(), equalTo(GcpCredentialsSupplier.class));
    }
}
