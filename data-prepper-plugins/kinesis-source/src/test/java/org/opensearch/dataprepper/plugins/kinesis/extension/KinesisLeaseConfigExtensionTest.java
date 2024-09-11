/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.extension;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class KinesisLeaseConfigExtensionTest {
    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private KinesisLeaseConfig kinesisLeaseConfig;

    private KinesisLeaseConfigExtension createObjectUnderTest() {
        return new KinesisLeaseConfigExtension(kinesisLeaseConfig);
    }

    @Test
    void applyShouldAddExtensionProvider() {
        extensionPoints = mock(ExtensionPoints.class);
        createObjectUnderTest().apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);

        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();

        assertThat(actualExtensionProvider, instanceOf(KinesisLeaseConfigProvider.class));
    }
}
