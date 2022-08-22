/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PeerForwarderAppConfig {

    @Bean
    public PeerForwarderConfiguration peerForwarderConfiguration(
            @Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getPeerForwarderConfiguration() != null) {
                return dataPrepperConfiguration.getPeerForwarderConfiguration();
            }
        else
            return new PeerForwarderConfiguration();
    }

    @Bean
    public PeerClientPool peerClientPool() {
        return new PeerClientPool();
    }

    @Bean
    public PeerForwarderClientFactory peerForwarderClientFactory(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerClientPool peerClientPool
    ) {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool);
    }

    @Bean
    public PeerForwarderClient peerForwarderClient(final ObjectMapper objectMapper) {
        return new PeerForwarderClient(objectMapper);
    }

    @Bean
    public PeerForwarder peerForwarder(final PeerForwarderClientFactory peerForwarderClientFactory,
                                       final PeerForwarderClient peerForwarderSerializer) {
        return new PeerForwarder(peerForwarderClientFactory, peerForwarderSerializer);
    }

}
