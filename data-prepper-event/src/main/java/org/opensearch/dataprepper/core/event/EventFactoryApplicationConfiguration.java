/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.inject.Named;

@Configuration
class EventFactoryApplicationConfiguration {
    @Bean
    EventConfiguration eventConfiguration(@Autowired(required = false) final EventConfigurationContainer eventConfigurationContainer) {
        if(eventConfigurationContainer == null || eventConfigurationContainer.getEventConfiguration() == null)
            return EventConfiguration.defaultConfiguration();
        return eventConfigurationContainer.getEventConfiguration();
    }

    @Bean(name = "innerEventKeyFactory")
    EventKeyFactory innerEventKeyFactory() {
        return new DefaultEventKeyFactory();
    }

    @Primary
    @Bean(name = "eventKeyFactory")
    EventKeyFactory eventKeyFactory(
            @Named("innerEventKeyFactory") final EventKeyFactory eventKeyFactory,
            final EventConfiguration eventConfiguration) {
        if(eventConfiguration.getMaximumCachedKeys() <= 0) {
            return eventKeyFactory;
        }
        return new CachingEventKeyFactory(eventKeyFactory, eventConfiguration);
    }
}
