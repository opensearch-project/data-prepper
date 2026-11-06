/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.breaker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Clock;
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
 * {@code check_interval} on a dedicated thread and, once open, does not check again before
 * {@code reset} has passed. {@link #isOpen()} only reads the current state.
 * <p>
 * Garbage collection after opening: the used heap includes garbage that the JVM has not collected
 * yet, for example records that were promoted to the old generation while they waited in a buffer.
 * When the used heap exceeds {@code usage}, the breaker opens first, so that requests are rejected
 * while the collection runs, and then requests a garbage collection ({@link System#gc()}). If the
 * collection brings the used heap at or below the close threshold, the breaker closes again in the
 * same check, without waiting for {@code reset}. If the breaker opens again within 5 seconds of the
 * last requested collection, it does not request another one.
 * <p>
 * Garbage collection while open: while the breaker stays open and the used heap is still above the
 * close threshold, it requests another garbage collection 5 seconds after the last one. Each time a
 * garbage collection does not bring the used heap down to the close threshold, the interval doubles,
 * up to 60 seconds. When the breaker opens again, the interval starts at 5 seconds again. This
 * prevents the breaker from staying open indefinitely because of garbage in the old generation that
 * young collections do not reclaim. Each request is logged with the heap usage before and after the
 * collection.
 * <p>
 * Example with the defaults: the used heap exceeds {@code usage} at t=0 s and garbage collections do
 * not bring it down to the close threshold.
 * <pre>
 * t =   0 s  open, garbage collection, still above close threshold, next in 5 s
 * t =   5 s  garbage collection, still above close threshold, next in 10 s
 * t =  15 s  garbage collection, still above close threshold, next in 20 s
 * t =  35 s  garbage collection, still above close threshold, next in 40 s
 * t =  75 s  garbage collection, still above close threshold, next in 60 s
 * t = 135 s  garbage collection, still above close threshold, next in 60 s
 * t = 195 s  ... every 60 s until the heap falls at or below the close threshold
 * </pre>
 * If the heap falls at or below the close threshold without a requested collection, the breaker
 * closes without one. The actual times can be up to one {@code check_interval} later.
 * With {@code -XX:+DisableExplicitGC} the requests have no effect.
 * <p>
 * Metrics: {@code core.circuitBreakers.heap.open} (gauge, 1 while open),
 * {@code core.circuitBreakers.heap.memoryUsage} (gauge, used heap bytes),
 * {@code core.circuitBreakers.heap.trips} (counter, each transition from closed to open, including
 * trips that a garbage collection resolves in the same check) and
 * {@code core.circuitBreakers.heap.garbageCollection} (timer, each requested garbage collection and
 * how long it took).
 *
 * @since 2.1
 */
class HeapCircuitBreaker implements InnerCircuitBreaker, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HeapCircuitBreaker.class);
    public static final int OPEN_METRIC_VALUE = 1;
    public static final int CLOSED_METRIC_VALUE = 0;
    static final String MEMORY_USAGE_METRIC = "core.circuitBreakers.heap.memoryUsage";
    static final String OPEN_METRIC = "core.circuitBreakers.heap.open";
    static final String TRIPS_METRIC = "core.circuitBreakers.heap.trips";
    static final String GARBAGE_COLLECTION_METRIC = "core.circuitBreakers.heap.garbageCollection";
    private static final double BYTES_PER_MEBIBYTE = 1024.0 * 1024.0;
    static final Duration DEFAULT_INITIAL_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(5);
    static final Duration DEFAULT_MAX_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(60);
    private final MemoryMXBean memoryMXBean;
    private final Runnable garbageCollector;
    private final Clock clock;
    private final Duration initialGarbageCollectionInterval;
    private final Duration maxGarbageCollectionInterval;
    private final long maxBytesToUse;
    private final long closeThresholdBytes;
    private final Duration resetPeriod;
    private final AtomicInteger openGauge;
    private final Counter tripsCounter;
    private final Timer garbageCollectionTimer;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean open;
    private Instant resetTime;
    private Duration garbageCollectionInterval;
    private Instant lastGarbageCollectionTime;

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig) {
        this(circuitBreakerConfig,
             ManagementFactory.getMemoryMXBean(),
             System::gc,
             Metrics.globalRegistry,
             Clock.systemUTC(),
             Executors.newSingleThreadScheduledExecutor(),
             DEFAULT_INITIAL_GARBAGE_COLLECTION_INTERVAL,
             DEFAULT_MAX_GARBAGE_COLLECTION_INTERVAL);
    }

    HeapCircuitBreaker(final HeapCircuitBreakerConfig circuitBreakerConfig,
                       final MemoryMXBean memoryMXBean,
                       final Runnable garbageCollector,
                       final MeterRegistry meterRegistry,
                       final Clock clock,
                       final ScheduledExecutorService scheduledExecutorService,
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
        final Duration checkInterval = Objects.requireNonNull(circuitBreakerConfig.getCheckInterval());
        this.memoryMXBean = memoryMXBean;
        this.garbageCollector = Objects.requireNonNull(garbageCollector);
        this.clock = Objects.requireNonNull(clock);
        this.initialGarbageCollectionInterval = Objects.requireNonNull(initialGarbageCollectionInterval);
        this.maxGarbageCollectionInterval = Objects.requireNonNull(maxGarbageCollectionInterval);
        open = false;
        resetTime = Instant.MIN;
        garbageCollectionInterval = initialGarbageCollectionInterval;
        lastGarbageCollectionTime = Instant.MIN;

        meterRegistry.gauge(MEMORY_USAGE_METRIC, this, cb -> getUsedMemoryBytes());
        openGauge = meterRegistry.gauge(OPEN_METRIC, new AtomicInteger(CLOSED_METRIC_VALUE));
        tripsCounter = meterRegistry.counter(TRIPS_METRIC);
        garbageCollectionTimer = meterRegistry.timer(GARBAGE_COLLECTION_METRIC);

        this.scheduledExecutorService = Objects.requireNonNull(scheduledExecutorService);
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
        final Instant now = clock.instant();
        if(open) {
            checkMemoryWhileOpen(now);
        } else {
            checkMemoryWhileClosed(now);
        }
    }

    private void checkMemoryWhileClosed(final Instant now) {
        final long bytesInUse = getUsedMemoryBytes();
        if(bytesInUse <= maxBytesToUse) {
            return;
        }

        trip(bytesInUse, now);
        if(hasGarbageCollectionIntervalElapsed(now)) {
            final long bytesAfter = collectGarbage(bytesInUse, now);
            if(bytesAfter <= closeThresholdBytes) {
                closeBreaker(bytesAfter);
            } else {
                LOG.info("Circuit breaker remains open after garbage collection. Next garbage collection in {} s.",
                        garbageCollectionInterval.toSeconds());
            }
        }
    }

    private void checkMemoryWhileOpen(final Instant now) {
        if(now.isBefore(resetTime)) {
            return;
        }

        long bytesInUse = getUsedMemoryBytes();
        if(bytesInUse > closeThresholdBytes && hasGarbageCollectionIntervalElapsed(now)) {
            bytesInUse = retryGarbageCollection(bytesInUse, now);
        }
        if(bytesInUse <= closeThresholdBytes) {
            closeBreaker(bytesInUse);
        }
    }

    private void trip(final long bytesInUse, final Instant now) {
        open = true;
        openGauge.set(OPEN_METRIC_VALUE);
        tripsCounter.increment();
        resetTime = now.plus(resetPeriod);
        garbageCollectionInterval = initialGarbageCollectionInterval;
        LOG.info("Circuit breaker tripped and open. {} used > {} configured",
                toReadableSize(bytesInUse),
                toReadableSize(maxBytesToUse));
    }

    private void closeBreaker(final long bytesInUse) {
        open = false;
        openGauge.set(CLOSED_METRIC_VALUE);
        LOG.info("Circuit breaker closed. {} used <= {} configured close threshold",
                toReadableSize(bytesInUse),
                toReadableSize(closeThresholdBytes));
    }

    private boolean hasGarbageCollectionIntervalElapsed(final Instant now) {
        final Instant nextGarbageCollectionTime = lastGarbageCollectionTime.plus(garbageCollectionInterval);
        return now.isAfter(nextGarbageCollectionTime) || now.equals(nextGarbageCollectionTime);
    }

    /**
     * Requests another garbage collection while the circuit breaker stays open and doubles the interval
     * up to the maximum while a collection does not bring the usage down to the close threshold.
     * See the class documentation for the schedule.
     */
    private long retryGarbageCollection(final long bytesBefore, final Instant now) {
        final long bytesAfter = collectGarbage(bytesBefore, now);
        if(bytesAfter > closeThresholdBytes) {
            final Duration doubledInterval = garbageCollectionInterval.multipliedBy(2);
            garbageCollectionInterval = doubledInterval.compareTo(maxGarbageCollectionInterval) < 0
                    ? doubledInterval
                    : maxGarbageCollectionInterval;
            LOG.info("Circuit breaker remains open after garbage collection. Next garbage collection in {} s.",
                    garbageCollectionInterval.toSeconds());
        }
        return bytesAfter;
    }

    private long collectGarbage(final long bytesBefore, final Instant now) {
        lastGarbageCollectionTime = now;
        garbageCollectionTimer.record(garbageCollector);
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
