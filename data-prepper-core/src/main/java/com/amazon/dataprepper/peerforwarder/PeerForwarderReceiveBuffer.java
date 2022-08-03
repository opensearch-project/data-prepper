/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.buffer.SizeOverflowException;
import com.amazon.dataprepper.model.record.Record;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * Buffer created for each stateful processor which implements {@link com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding}
 * interface.
 *
 * @since 2.0
 */
public class PeerForwarderReceiveBuffer<T extends Record<?>> implements Buffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderReceiveBuffer.class);

    private final int bufferSize;
    private final int batchSize;
    private final String pluginName;
    private final Semaphore capacitySemaphore;
    private final LinkedBlockingQueue<T> blockingQueue;
    private int recordsInFlight = 0;

    public PeerForwarderReceiveBuffer(final int bufferSize, final int batchSize, final String pluginName) {
        this.bufferSize = bufferSize;
        this.batchSize = batchSize;
        this.pluginName = pluginName;
        this.blockingQueue = new LinkedBlockingQueue<>(bufferSize);
        this.capacitySemaphore = new Semaphore(bufferSize);
    }

    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException(format("Plugin [%s] - Buffer is full, timed out waiting for a slot",
                        pluginName));
            }
            blockingQueue.offer(record);
        } catch (InterruptedException ex) {
            LOG.error("Plugin [{}] - Buffer is full, interrupted while waiting to write the record", pluginName, ex);
            throw new TimeoutException("Buffer is full, timed out waiting for a slot");
        }
    }

    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        final int size = records.size();
        if (size > bufferSize) {
            throw new SizeOverflowException(format("Buffer capacity too small for the size of records: %d", size));
        }
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(size, timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException(
                        format("Plugin [%s] - Buffer does not have enough capacity left for the size of records: %d, " +
                                        "timed out waiting for slots.",
                                pluginName, size));
            }
            blockingQueue.addAll(records);
        } catch (InterruptedException ex) {
            LOG.error("Plugin [{}] - Buffer does not have enough capacity left for the size of records: {}, " +
                            "interrupted while waiting to write the records",
                    pluginName, size, ex);
            throw new TimeoutException(
                    format("Plugin [%s] - Buffer does not have enough capacity left for the size of records: %d, " +
                                    "timed out waiting for slots.",
                            pluginName, size));
        }
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        final List<T> records = new ArrayList<>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutInMillis && records.size() < batchSize) {
                final T record = blockingQueue.poll(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (record != null) { //record can be null, avoiding adding nulls
                    records.add(record);
                }
                if (records.size() < batchSize) {
                    blockingQueue.drainTo(records, batchSize - records.size());
                }
            }
        } catch (InterruptedException ex) {
            LOG.info("Plugin [{}] - Interrupt received while reading from buffer", pluginName);
            throw new RuntimeException(ex);
        }
        final CheckpointState checkpointState = new CheckpointState(records.size());
        recordsInFlight += checkpointState.getNumRecordsToBeChecked();
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    @Override
    public void checkpoint(CheckpointState checkpointState) {
        final int numCheckedRecords = checkpointState.getNumRecordsToBeChecked();
        capacitySemaphore.release(numCheckedRecords);
        recordsInFlight -= checkpointState.getNumRecordsToBeChecked();
    }

    @Override
    public boolean isEmpty() {
        return blockingQueue.isEmpty() && recordsInFlight == 0;
    }
}
