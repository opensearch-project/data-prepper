/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EventFactoryApplicationConfiguration {
    @Bean
    EventConfiguration eventConfiguration(@Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if(dataPrepperConfiguration == null || dataPrepperConfiguration.getEventConfiguration() == null)
            return EventConfiguration.defaultConfiguration();
        return dataPrepperConfiguration.getEventConfiguration();
    }
}
