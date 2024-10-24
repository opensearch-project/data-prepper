/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PeerForwarderAppConfigTest {

    private static final PeerForwarderAppConfig peerForwarderAppConfig = new PeerForwarderAppConfig();

    @Test
    void peerForwarderConfiguration_with_non_null_DataPrepperConfiguration_should_return_PeerForwarderConfiguration() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getPeerForwarderConfiguration()).thenReturn(mock(PeerForwarderConfiguration.class));

        final PeerForwarderConfiguration peerForwarderConfiguration = peerForwarderAppConfig.peerForwarderConfiguration(dataPrepperConfiguration);

        verify(dataPrepperConfiguration, times(2)).getPeerForwarderConfiguration();
        assertThat(peerForwarderConfiguration, notNullValue());
    }

    @Test
    void peerForwarderConfiguration_with_null_DataPrepperConfiguration_should_return_default_PeerForwarderConfiguration() {
        final DataPrepperConfiguration dataPrepperConfiguration = null;
        final PeerForwarderConfiguration peerForwarderConfiguration = peerForwarderAppConfig.peerForwarderConfiguration(dataPrepperConfiguration);

        assertThat(peerForwarderConfiguration, notNullValue());
    }

    @Test
    void peerClientPool_should_return_test() {
        PeerClientPool peerClientPool = peerForwarderAppConfig.peerClientPool();

        assertThat(peerClientPool, notNullValue());
    }

    @Test
    void peerForwarderClientFactory_should_return_test() {
        PeerForwarderClientFactory peerForwarderClientFactory = peerForwarderAppConfig.peerForwarderClientFactory(
                mock(PeerForwarderConfiguration.class),
                mock(PeerClientPool.class),
                mock(CertificateProviderFactory.class),
                mock(PluginMetrics.class)
        );

        assertThat(peerForwarderClientFactory, notNullValue());
    }

}