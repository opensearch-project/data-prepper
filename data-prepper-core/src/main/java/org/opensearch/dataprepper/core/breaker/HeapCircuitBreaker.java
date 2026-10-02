/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.breaker;

import io.micrometer.core.instrument.Metrics;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link CircuitBreaker} which checks against heap usage.
 *
 * @since 2.1
 */
class HeapCircuitBreaker implements InnerCircuitBreaker, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HeapCircuitBreaker.class);
    public static final int OPEN_METRIC_VALUE = 1;
    public static final int CLOSED_METRIC_VALUE = 0;
    private final MemoryMXBean memoryMXBean;
    private final long maxBytesToUse;
    private final long closeThresholdBytes;
    private final Duration resetPeriod;
    private final Lock lock;
    private final AtomicInteger openGauge;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean open;
    private Instant resetTime;

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig) {
        this(circuitBreakerConfig, ManagementFactory.getMemoryMXBean());
    }

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig, final MemoryMXBean memoryMXBean) {
        Objects.requireNonNull(circuitBreakerConfig);
        Objects.requireNonNull(circuitBreakerConfig.getUsage());

        maxBytesToUse = circuitBreakerConfig.getUsage().getBytes();
        if(maxBytesToUse <= 0)
            throw new IllegalArgumentException("Bytes usage must be positive.");

        if (circuitBreakerConfig.getCloseUsage() != null) {
            closeThresholdBytes = circuitBreakerConfig.getCloseUsage().getBytes();
            if (closeThresholdBytes > maxBytesToUse) {
                throw new IllegalArgumentException("close_usage must be less than or equal to usage.");
            }
            if (closeThresholdBytes <= 0) {
                throw new IllegalArgumentException("close_usage must be positive.");
            }
        } else {
            closeThresholdBytes = maxBytesToUse;
        }

        resetPeriod = Objects.requireNonNull(circuitBreakerConfig.getReset());
        this.memoryMXBean = memoryMXBean;
        open = false;
        lock = new ReentrantLock();
        resetTime = Instant.MIN;

        Metrics.gauge("core.circuitBreakers.heap.memoryUsage", this, cb -> getUsedMemoryBytes());
        openGauge = Metrics.gauge("core.circuitBreakers.heap.open", new AtomicInteger(0));

        final Duration checkInterval = Objects.requireNonNull(circuitBreakerConfig.getCheckInterval());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::checkMemory, 0L, checkInterval.toMillis(), TimeUnit.MILLISECONDS);

        LOG.info("Circuit breaker heap open threshold is {} bytes, close threshold is {} bytes.", maxBytesToUse, closeThresholdBytes);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private void checkMemory() {
        final boolean previousOpen = open;

        if(previousOpen && Instant.now().compareTo(resetTime) < 0) {
            return;
        }

        final long bytesInUse = getUsedMemoryBytes();
        if(bytesInUse > maxBytesToUse) {
            open = true;
            if(!previousOpen) {
                System.gc();
                resetTime = Instant.now().plus(resetPeriod);
                openGauge.set(OPEN_METRIC_VALUE);
                LOG.info("Circuit breaker tripped and open. {} used memory bytes > {} configured", bytesInUse, maxBytesToUse);
            }
        } else if (bytesInUse <= closeThresholdBytes) {
            // Only close when usage falls below the (potentially lower) close threshold
            open = false;
            if(previousOpen) {
                openGauge.set(CLOSED_METRIC_VALUE);
                LOG.info("Circuit breaker closed. {} used memory bytes <= {} configured close threshold", bytesInUse, closeThresholdBytes);
            }
        }
    }

    private long getUsedMemoryBytes() {
        return memoryMXBean.getHeapMemoryUsage().getUsed();
    }

    @Override
    public void close() throws Exception {
        scheduledExecutorService.shutdown();
    }
}
