/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The application config for circuit breakers. Used for wiring beans
 * related to circuit breakers.
 *
 * @since 2.1
 */
@Configuration
public class CircuitBreakerAppConfig {
    @Bean
    public CircuitBreakerService circuitBreakerService(final DataPrepperConfiguration dataPrepperConfiguration) {
        return new CircuitBreakerService(dataPrepperConfiguration.getCircuitBreakerConfig());
    }
}
