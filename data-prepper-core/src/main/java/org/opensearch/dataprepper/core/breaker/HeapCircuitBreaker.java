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
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link CircuitBreaker} which checks against heap usage.
 * <p>
 * The breaker opens when the used heap exceeds {@code usage} and closes when it falls at or below
 * {@code close_usage} (or {@code usage} if {@code close_usage} is not set). It checks the heap every
 * {@code check_interval} and, once open, does not check again before {@code reset} has passed.
 * <p>
 * Garbage collection before opening: the used heap includes garbage that the JVM has not collected
 * yet, for example records that were promoted to the old generation while they waited in a buffer.
 * When the used heap exceeds {@code usage} while the breaker is closed, the breaker first requests a
 * garbage collection ({@link System#gc()}) and opens only if the used heap is still above
 * {@code usage} afterwards. If the used heap exceeds {@code usage} again within 5 seconds of the last
 * requested collection, the breaker opens without another collection.
 * <p>
 * Garbage collection while open: while the breaker stays open and the used heap is still above the
 * close threshold, it requests another garbage collection 5 seconds after the last one. Each time a
 * garbage collection does not bring the used heap down to the close threshold, the interval doubles,
 * up to 60 seconds. When the breaker opens again, the interval starts at 5 seconds again. This
 * prevents the breaker from staying open indefinitely because of garbage in the old generation that
 * young collections do not reclaim. Each request is logged with the heap usage before and after the
 * collection.
 * <p>
 * Example with the defaults: the used heap exceeds {@code usage} at t=0 s, the garbage collection
 * does not bring it down to {@code usage}, and the heap stays above the close threshold.
 * <pre>
 * t =   0 s  garbage collection, still above usage, open
 * t =   5 s  garbage collection, still above close threshold, next in 10 s
 * t =  15 s  garbage collection, still above close threshold, next in 20 s
 * t =  35 s  garbage collection, still above close threshold, next in 40 s
 * t =  75 s  garbage collection, still above close threshold, next in 60 s
 * t = 135 s  garbage collection, still above close threshold, next in 60 s
 * t = 195 s  ... every 60 s until the heap falls at or below the close threshold
 * </pre>
 * If a garbage collection brings the heap at or below the close threshold, the breaker closes in the
 * same check. If the heap falls at or below the close threshold without a requested collection, the
 * breaker closes without one. The actual times can be up to one {@code check_interval} later.
 * With {@code -XX:+DisableExplicitGC} the requests have no effect.
 *
 * @since 2.1
 */
class HeapCircuitBreaker implements InnerCircuitBreaker, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HeapCircuitBreaker.class);
    public static final int OPEN_METRIC_VALUE = 1;
    public static final int CLOSED_METRIC_VALUE = 0;
    private static final double BYTES_PER_MEBIBYTE = 1024.0 * 1024.0;
    static final Duration DEFAULT_INITIAL_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(5);
    static final Duration DEFAULT_MAX_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(60);
    private final MemoryMXBean memoryMXBean;
    private final Runnable garbageCollector;
    private final Duration initialGarbageCollectionInterval;
    private final Duration maxGarbageCollectionInterval;
    private final long maxBytesToUse;
    private final long closeThresholdBytes;
    private final Duration resetPeriod;
    private final AtomicInteger openGauge;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean open;
    private Instant resetTime;
    private Duration garbageCollectionInterval;
    private Instant nextGarbageCollectionTime;

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig) {
        this(circuitBreakerConfig,
             ManagementFactory.getMemoryMXBean(),
             System::gc,
             DEFAULT_INITIAL_GARBAGE_COLLECTION_INTERVAL,
             DEFAULT_MAX_GARBAGE_COLLECTION_INTERVAL);
    }

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig,
                       final MemoryMXBean memoryMXBean,
                       final Runnable garbageCollector,
                       final Duration initialGarbageCollectionInterval,
                       final Duration maxGarbageCollectionInterval) {
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
        this.garbageCollector = Objects.requireNonNull(garbageCollector);
        this.initialGarbageCollectionInterval = Objects.requireNonNull(initialGarbageCollectionInterval);
        this.maxGarbageCollectionInterval = Objects.requireNonNull(maxGarbageCollectionInterval);
        open = false;
        resetTime = Instant.MIN;
        garbageCollectionInterval = initialGarbageCollectionInterval;
        nextGarbageCollectionTime = Instant.MIN;

        Metrics.gauge("core.circuitBreakers.heap.memoryUsage", this, cb -> getUsedMemoryBytes());
        openGauge = Metrics.gauge("core.circuitBreakers.heap.open", new AtomicInteger(0));

        final Duration checkInterval = Objects.requireNonNull(circuitBreakerConfig.getCheckInterval());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::checkMemory, 0L, checkInterval.toMillis(), TimeUnit.MILLISECONDS);

        LOG.info("Circuit breaker heap open threshold is {}, close threshold is {}.",
                toReadableSize(maxBytesToUse),
                toReadableSize(closeThresholdBytes));
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

        long bytesInUse = getUsedMemoryBytes();
        final boolean garbageCollectionDue = Instant.now().compareTo(nextGarbageCollectionTime) >= 0;
        if(previousOpen && bytesInUse > closeThresholdBytes && garbageCollectionDue) {
            bytesInUse = retryGarbageCollection(bytesInUse);
        } else if(!previousOpen && bytesInUse > maxBytesToUse && garbageCollectionDue) {
            bytesInUse = collectGarbageBeforeTripping(bytesInUse);
        }

        if(bytesInUse > maxBytesToUse) {
            open = true;
            if(!previousOpen) {
                openGauge.set(OPEN_METRIC_VALUE);
                LOG.info("Circuit breaker tripped and open. {} used > {} configured",
                        toReadableSize(bytesInUse),
                        toReadableSize(maxBytesToUse));
                resetTime = Instant.now().plus(resetPeriod);
                garbageCollectionInterval = initialGarbageCollectionInterval;
                final Instant nextGarbageCollectionAfterTrip = Instant.now().plus(garbageCollectionInterval);
                if(nextGarbageCollectionAfterTrip.isBefore(nextGarbageCollectionTime)) {
                    nextGarbageCollectionTime = nextGarbageCollectionAfterTrip;
                }
            }
        } else if (bytesInUse <= closeThresholdBytes) {
            // Only close when usage is at or below the (potentially lower) close threshold
            open = false;
            if(previousOpen) {
                openGauge.set(CLOSED_METRIC_VALUE);
                LOG.info("Circuit breaker closed. {} used <= {} configured close threshold",
                        toReadableSize(bytesInUse),
                        toReadableSize(closeThresholdBytes));
            }
        }
    }

    /**
     * Requests another garbage collection while the circuit breaker stays open and doubles the interval
     * up to the maximum while a collection does not bring the usage down to the close threshold.
     * See the class documentation for the schedule.
     */
    private long retryGarbageCollection(final long bytesBefore) {
        final long bytesAfter = collectGarbage(bytesBefore);
        if(bytesAfter > closeThresholdBytes) {
            final Duration doubledInterval = garbageCollectionInterval.multipliedBy(2);
            garbageCollectionInterval = doubledInterval.compareTo(maxGarbageCollectionInterval) < 0
                    ? doubledInterval
                    : maxGarbageCollectionInterval;
            LOG.info("Circuit breaker remains open after garbage collection. Next garbage collection in {} s.",
                    garbageCollectionInterval.toSeconds());
        }
        nextGarbageCollectionTime = Instant.now().plus(garbageCollectionInterval);
        return bytesAfter;
    }

    private long collectGarbageBeforeTripping(final long bytesBefore) {
        final long bytesAfter = collectGarbage(bytesBefore);
        nextGarbageCollectionTime = Instant.now().plus(initialGarbageCollectionInterval);
        if(bytesAfter <= maxBytesToUse) {
            LOG.info("Circuit breaker did not trip. {} used after garbage collection <= {} configured",
                    toReadableSize(bytesAfter),
                    toReadableSize(maxBytesToUse));
        }
        return bytesAfter;
    }

    private long collectGarbage(final long bytesBefore) {
        garbageCollector.run();
        final long bytesAfter = getUsedMemoryBytes();
        LOG.info("Circuit breaker requested garbage collection. Heap used before {}, after {}, freed {}.",
                toReadableSize(bytesBefore),
                toReadableSize(bytesAfter),
                toReadableSize(bytesBefore - bytesAfter));
        return bytesAfter;
    }

    private static String toReadableSize(final long bytes) {
        return String.format(Locale.ROOT, "%.1f MiB (%d bytes)", bytes / BYTES_PER_MEBIBYTE, bytes);
    }

    private long getUsedMemoryBytes() {
        return memoryMXBean.getHeapMemoryUsage().getUsed();
    }

    @Override
    public void close() throws Exception {
        scheduledExecutorService.shutdown();
    }
}
