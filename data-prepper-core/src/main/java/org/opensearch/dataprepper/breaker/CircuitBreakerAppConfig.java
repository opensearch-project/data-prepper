/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import org.opensearch.dataprepper.parser.model.CircuitBreakerConfig;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The application config for circuit breakers. Used for wiring beans
 * related to circuit breakers.
 *
 * @since 2.1
 */
@Configuration
public class CircuitBreakerAppConfig {
    @Bean
    public CircuitBreakerManager circuitBreakerService(final List<InnerCircuitBreaker> circuitBreakers) {
        return new CircuitBreakerManager(circuitBreakers);
    }

    @Bean
    InnerCircuitBreaker heapCircuitBreaker(final DataPrepperConfiguration dataPrepperConfiguration) {
        final CircuitBreakerConfig circuitBreakerConfig = dataPrepperConfiguration.getCircuitBreakerConfig();
        if(circuitBreakerConfig != null && circuitBreakerConfig.getHeapConfig() != null) {
            return new HeapCircuitBreaker(circuitBreakerConfig.getHeapConfig());
        } else {
            return null;
        }
    }
}
