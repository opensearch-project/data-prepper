/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;

import java.time.Instant;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract implementation of the Buffer interface to record boilerplate metrics
 */
public abstract class AbstractBuffer<T extends Record<?>> implements Buffer<T> {
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsWrittenCounter;
    private final Counter recordsReadCounter;
    private final AtomicLong recordsInFlight;
    private final AtomicLong recordsInBuffer;
    private final Counter recordsProcessedCounter;
    private final Counter writeTimeoutCounter;
    private final Counter recordsWriteFailed;
    private final Timer writeTimer;
    private final Timer latencyTimer;
    private final Timer readTimer;
    private final Timer checkpointTimer;

    public AbstractBuffer(final PluginSetting pluginSetting) {
        this(PluginMetrics.fromPluginSetting(pluginSetting), pluginSetting.getPipelineName());
    }

    public AbstractBuffer(final String bufferName, final String pipelineName) {
        this(PluginMetrics.fromNames(bufferName, pipelineName), pipelineName);
    }

    private AbstractBuffer(final PluginMetrics pluginMetrics, final String pipelineName) {
        this.pluginMetrics = pluginMetrics;
        this.recordsWrittenCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.recordsReadCounter = pluginMetrics.counter(MetricNames.RECORDS_READ);
        this.recordsInFlight = pluginMetrics.gauge(MetricNames.RECORDS_INFLIGHT, new AtomicLong());
        this.recordsInBuffer = pluginMetrics.gauge(MetricNames.RECORDS_IN_BUFFER, new AtomicLong());
        this.recordsProcessedCounter = pluginMetrics.counter(MetricNames.RECORDS_PROCESSED, pipelineName);
        this.recordsWriteFailed = pluginMetrics.counter(MetricNames.RECORDS_WRITE_FAILED);
        this.writeTimeoutCounter = pluginMetrics.counter(MetricNames.WRITE_TIMEOUTS);
        this.writeTimer = pluginMetrics.timer(MetricNames.WRITE_TIME_ELAPSED);
        this.readTimer = pluginMetrics.timer(MetricNames.READ_TIME_ELAPSED);
        this.latencyTimer = pluginMetrics.timer(MetricNames.READ_LATENCY);
        this.checkpointTimer = pluginMetrics.timer(MetricNames.CHECKPOINT_TIME_ELAPSED);
    }

    /**
     * Records metrics for ingress, time elapsed, and timeouts, while calling the doWrite method
     * to perform the actual write
     *
     * @param record          the Record to add
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException Exception thrown when the operation times out
     */
    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        long startTime = System.nanoTime();

        try {
            doWrite(record, timeoutInMillis);
            if (!isByteBuffer()) {
                recordsWrittenCounter.increment();
                recordsInBuffer.incrementAndGet();
            }
            postProcess(recordsInBuffer.get());
        } catch (TimeoutException e) {
            recordsWriteFailed.increment();
            writeTimeoutCounter.increment();
            throw e;
        } finally {
            writeTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Records metrics for ingress, time elapsed, and timeouts, while calling the doWriteAll method
     * to perform the actual write
     *
     * @param records          the collection of Record to add
     * @param timeoutInMillis  how long to wait before giving up
     * @throws Exception       Exception passed up
     */
    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        long startTime = System.nanoTime();

        final int size = records.size();
        try {
            doWriteAll(records, timeoutInMillis);
            // we do not know how many records when the buffer is bytebuffer
            if (!isByteBuffer()) {
                recordsWrittenCounter.increment(size);
                recordsInBuffer.addAndGet(size);
            }
            postProcess(recordsInBuffer.get());
        } catch (Exception e) {
            recordsWriteFailed.increment(size);
            if (e instanceof TimeoutException) {
                writeTimeoutCounter.increment();
            }
            throw e;
        } finally {
            writeTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void writeBytes(final byte[] bytes, final String key, int timeoutInMillis) throws Exception {
        throw new RuntimeException("not supported");
    }

    /**
     * Records egress and time elapsed metrics, while calling the doRead function to
     * do the actual read
     *
     * @param timeoutInMillis how long to wait before giving up
     * @return Records collection and checkpoint state read from the buffer
     */
    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        final Map.Entry<Collection<T>, CheckpointState> readResult = readTimer.record(() -> doRead(timeoutInMillis));
        // we do not know how many records when the buffer is bytebuffer
        if (!isByteBuffer()) {
            recordsReadCounter.increment(readResult.getKey().size() * 1.0);
            recordsInFlight.addAndGet(readResult.getValue().getNumRecordsToBeChecked());
            recordsInBuffer.addAndGet(-1 * readResult.getValue().getNumRecordsToBeChecked());
        }
        postProcess(recordsInBuffer.get());
        return readResult;
    }

    protected void updateLatency(Collection<T> records) {
        for (T rec : records) {
            if (rec instanceof Record) {
                Object data = rec.getData();
                if (data instanceof Event) {
                    Event event = (Event) data;
                    Instant receivedTime = event.getEventHandle().getInternalOriginationTime();
                    latencyTimer.record(Duration.between(receivedTime, Instant.now()));
                }
            }
        }
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        checkpointTimer.record(() -> doCheckpoint(checkpointState));
        if (!isByteBuffer()) {
            final int numRecordsToBeChecked = checkpointState.getNumRecordsToBeChecked();
            recordsInFlight.addAndGet(-numRecordsToBeChecked);
            recordsProcessedCounter.increment(numRecordsToBeChecked);
        }
    }

    public int getRecordsInFlight() {
        return recordsInFlight.intValue();
    }

    /**
     * This method should implement the logic for writing to the buffer
     *
     * @param record          Record to write to buffer
     * @param timeoutInMillis Timeout for write operation in millis
     * @throws TimeoutException Exception thrown when the operation times out
     */
    public abstract void doWrite(T record, int timeoutInMillis) throws TimeoutException;

    /**
     * This method should implement the logic for writing to the buffer
     *
     * @param records          Collection of records to write to buffer
     * @param timeoutInMillis Timeout for write operation in millis
     * @throws Exception Exception thrown when the operation times out
     */
    public abstract void doWriteAll(Collection<T> records, int timeoutInMillis) throws Exception;

    /**
     * This method should implement the logic for reading from the buffer
     *
     * @param timeoutInMillis Timeout in millis
     * @return Records collection and checkpoint state read from the buffer
     */
    public abstract Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis);

    public abstract void doCheckpoint(CheckpointState checkpointState);

    public abstract boolean isEmpty();

    /**
     * This method is run after the core processing is complete in read, write, and writeAll. This is a hook
     * provides the current recordsInBuffer. Default implementation is a no-op.
     *
     * @param recordsInBuffer the current number of records in the buffer
     */
    public void postProcess(final Long recordsInBuffer) {

    }
}
