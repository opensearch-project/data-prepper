/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.opensearch.dataprepper.core.parser.model.AcknowledgementsConfig;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

@Configuration
class AcknowledgementAppConfig {
    private static final int MAX_THREADS = 12;

    @Bean
    AcknowledgementsConfig acknowledgementsConfig(@Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getAcknowledgementsConfig() != null) {
            return dataPrepperConfiguration.getAcknowledgementsConfig();
        }

        return AcknowledgementsConfig.defaultConfiguration();
    }

    @Bean
    CallbackTheadFactory callbackTheadFactory() {
        final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        return new CallbackTheadFactory(defaultThreadFactory);
    }

    @Bean(name = "acknowledgementCallbackExecutor")
    ScheduledExecutorService acknowledgementCallbackExecutor(final CallbackTheadFactory callbackTheadFactory) {
        return Executors.newScheduledThreadPool(MAX_THREADS, callbackTheadFactory);
    }
}
