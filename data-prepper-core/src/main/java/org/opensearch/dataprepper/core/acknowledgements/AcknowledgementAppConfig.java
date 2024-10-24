/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

@Configuration
class AcknowledgementAppConfig {
    private static final int MAX_THREADS = 12;

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
