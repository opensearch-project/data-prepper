/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import io.micrometer.core.instrument.Metrics;
import org.opensearch.dataprepper.parser.model.HeapCircuitBreakerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link CircuitBreaker} which checks against heap usage.
 *
 * @since 2.1
 */
class HeapCircuitBreaker implements InnerCircuitBreaker {
    private static final Logger LOG = LoggerFactory.getLogger(HeapCircuitBreaker.class);
    private final MemoryMXBean memoryMXBean;
    private final long usageBytes;
    private final Duration resetPeriod;
    private final Lock lock;
    private final AtomicInteger openGauge;
    private boolean open;
    private Instant resetTime;

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig) {
        this(circuitBreakerConfig, ManagementFactory.getMemoryMXBean());
    }

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig, final MemoryMXBean memoryMXBean) {
        Objects.requireNonNull(circuitBreakerConfig);
        Objects.requireNonNull(circuitBreakerConfig.getUsage());

        usageBytes = circuitBreakerConfig.getUsage().getBytes();
        if(usageBytes <= 0)
            throw new IllegalArgumentException("Bytes usage must be positive.");

        resetPeriod = circuitBreakerConfig.getReset();
        this.memoryMXBean = memoryMXBean;
        open = false;
        lock = new ReentrantLock();
        resetTime = Instant.MIN;

        Metrics.gauge("core.circuitBreakers.heap.memoryUsage", this, cb -> getUsedMemoryBytes());
        openGauge = Metrics.gauge("core.circuitBreakers.heap.open", new AtomicInteger(0));

        LOG.info("Heap circuit breaker with usage of {} bytes.", usageBytes);
    }

    @Override
    public boolean isOpen() {
        if(lock.tryLock()) {
            try {
                checkMemory();
            } catch (final RuntimeException ex) {
                LOG.warn("Suppressing exception thrown while checking memory.", ex);
            } finally {
                lock.unlock();
            }
        }

        return open;
    }

    private void checkMemory() {
        final boolean previousOpen = open;

        if(previousOpen && Instant.now().compareTo(resetTime) < 0) {
            return;
        }

        final long usedMemoryBytes = getUsedMemoryBytes();
        if(usedMemoryBytes >  usageBytes) {
            open = true;
            if(!previousOpen) {
                resetTime = Instant.now().plus(resetPeriod);
                openGauge.set(1);
                LOG.info("Circuit breaker tripped and open. {} used > {} configured", usedMemoryBytes, usageBytes);
            }
        } else {
            open = false;
            if(previousOpen) {
                openGauge.set(0);
                LOG.info("Circuit breaker closed. {} used <= {} configured", usedMemoryBytes, usageBytes);
            }
        }
    }

    private long getUsedMemoryBytes() {
        return memoryMXBean.getHeapMemoryUsage().getUsed();
    }
}
