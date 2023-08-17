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
    private final long usageBytes;
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

        usageBytes = circuitBreakerConfig.getUsage().getBytes();
        if(usageBytes <= 0)
            throw new IllegalArgumentException("Bytes usage must be positive.");

        resetPeriod = Objects.requireNonNull(circuitBreakerConfig.getReset());
        this.memoryMXBean = memoryMXBean;
        open = false;
        lock = new ReentrantLock();
        resetTime = Instant.MIN;

        Metrics.gauge("core.circuitBreakers.heap.memoryUsage", this, cb -> getUsedMemoryBytes());
        openGauge = Metrics.gauge("core.circuitBreakers.heap.open", new AtomicInteger(0));

        final Duration checkInterval = Objects.requireNonNull(circuitBreakerConfig.getCheckInterval());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService
                        .scheduleAtFixedRate(this::checkMemory, 0L, checkInterval.toMillis(), TimeUnit.MILLISECONDS);

        LOG.info("Circuit breaker heap limit is set to {} bytes.", usageBytes);
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

        final long usedMemoryBytes = getUsedMemoryBytes();
        if(usedMemoryBytes >  usageBytes) {
            open = true;
            if(!previousOpen) {
                System.gc();
                resetTime = Instant.now().plus(resetPeriod);
                openGauge.set(OPEN_METRIC_VALUE);
                LOG.info("Circuit breaker tripped and open. {} used memory bytes > {} configured", usedMemoryBytes, usageBytes);
            }
        } else {
            open = false;
            if(previousOpen) {
                openGauge.set(CLOSED_METRIC_VALUE);
                LOG.info("Circuit breaker closed. {} used memory bytes <= {} configured", usedMemoryBytes, usageBytes);
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
