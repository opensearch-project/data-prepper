/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

/**
 * Represents if the {@link KafkaCustomConsumer} should pause consuming
 * from a Kafka topic due to an external reason.
 */
@FunctionalInterface
public interface PauseConsumePredicate {
    /**
     * Returns whether to pause consumption of a Kafka topic.
     *
     * @return True if the consumer should pause. False if there is it does not need to pause.
     */
    boolean pauseConsuming();

    /**
     * Returns a {@link PauseConsumePredicate} from a {@link CircuitBreaker}. This value may
     * be null, in which case, it will not pause.
     *
     * @param circuitBreaker The {@link CircuitBreaker} or <b>null</b>
     * @return a predicate based on the circuit breaker.
     */
    static PauseConsumePredicate circuitBreakingPredicate(final CircuitBreaker circuitBreaker) {
        if(circuitBreaker == null)
            return noPause();
        return circuitBreaker::isOpen;
    }

    /**
     * Returns a {@link PauseConsumePredicate} that never pauses.
     *
     * @return a predicate that does not pause
     */
    static PauseConsumePredicate noPause() {
        return () -> false;
    }
}
