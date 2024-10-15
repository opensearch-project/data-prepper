/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.certificate.CertificateProviderFactory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServerProviderTest {
    @Mock
    PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    CertificateProviderFactory certificateProviderFactory;

    @Mock
    PeerForwarderHttpService peerForwarderHttpService;

    private PeerForwarderHttpServerProvider createObjectUnderTest() {
        return new PeerForwarderHttpServerProvider(peerForwarderConfiguration, certificateProviderFactory, peerForwarderHttpService);
    }

    @Test
    void get_should_create_a_server() {
        when(peerForwarderConfiguration.getMaxConnectionCount()).thenReturn(500);
        final Server server = createObjectUnderTest().get();

        Assertions.assertNotNull(server);
        assertThat(server, instanceOf(Server.class));
    }

}