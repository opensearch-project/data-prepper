/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model.buffer;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

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
    private final Timer writeTimer;
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
        this.writeTimeoutCounter = pluginMetrics.counter(MetricNames.WRITE_TIMEOUTS);
        this.writeTimer = pluginMetrics.timer(MetricNames.WRITE_TIME_ELAPSED);
        this.readTimer = pluginMetrics.timer(MetricNames.READ_TIME_ELAPSED);
        this.checkpointTimer = pluginMetrics.timer(MetricNames.CHECKPOINT_TIME_ELAPSED);
    }

    /**
     * Records metrics for ingress, time elapsed, and timeouts, while calling the doWrite method
     * to perform the actual write
     *
     * @param record          the Record to add
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException
     */
    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        long startTime = System.nanoTime();

        try {
            doWrite(record, timeoutInMillis);
            recordsWrittenCounter.increment();
            recordsInBuffer.incrementAndGet();
        } catch (TimeoutException e) {
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
     * @param timeoutInMillis how long to wait before giving up
     * @throws Exception
     */
    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        long startTime = System.nanoTime();

        try {
            final int size = records.size();
            doWriteAll(records, timeoutInMillis);
            recordsWrittenCounter.increment(size);
            recordsInBuffer.addAndGet(size);
        } catch (Exception e) {
            if (e instanceof TimeoutException) {
                writeTimeoutCounter.increment();
            }
            throw e;
        } finally {
            writeTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
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
        recordsReadCounter.increment(readResult.getKey().size() * 1.0);
        recordsInFlight.addAndGet(readResult.getValue().getNumRecordsToBeChecked());
        recordsInBuffer.addAndGet(-1 * readResult.getValue().getNumRecordsToBeChecked());
        return readResult;
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        checkpointTimer.record(() -> doCheckpoint(checkpointState));
        final int numRecordsToBeChecked = checkpointState.getNumRecordsToBeChecked();
        recordsInFlight.addAndGet(-numRecordsToBeChecked);
        recordsProcessedCounter.increment(numRecordsToBeChecked);
    }

    protected int getRecordsInFlight() {
        return recordsInFlight.intValue();
    }

    /**
     * This method should implement the logic for writing to the  buffer
     *
     * @param record          Record to write to buffer
     * @param timeoutInMillis Timeout for write operation in millis
     * @throws TimeoutException
     */
    public abstract void doWrite(T record, int timeoutInMillis) throws TimeoutException;

    /**
     * This method should implement the logic for writing to the  buffer
     *
     * @param records          Collection of records to write to buffer
     * @param timeoutInMillis Timeout for write operation in millis
     * @throws Exception
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
}
