/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;

public class PeerForwarderAppConfig {

    @Bean
    public PeerForwarderConfiguration peerForwarderConfiguration(final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null)
            return dataPrepperConfiguration.getPeerForwarderConfiguration();
        else
            return new PeerForwarderConfiguration();
    }

}
