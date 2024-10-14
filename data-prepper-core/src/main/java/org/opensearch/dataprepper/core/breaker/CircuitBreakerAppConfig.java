/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.breaker;

import org.opensearch.dataprepper.core.parser.model.CircuitBreakerConfig;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

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

    @Bean
    public Optional<CircuitBreaker> circuitBreaker(final CircuitBreakerManager circuitBreakerManager) {
        return circuitBreakerManager.getGlobalCircuitBreaker();
    }
}
