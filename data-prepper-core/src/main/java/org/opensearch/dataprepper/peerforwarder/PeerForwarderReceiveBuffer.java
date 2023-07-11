/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * Buffer created for each stateful processor which implements {@link org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding}
 * interface.
 *
 * @since 2.0
 */
public class PeerForwarderReceiveBuffer<T extends Record<?>> extends AbstractBuffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderReceiveBuffer.class);

    private static final String CORE_PEER_FORWARDER_COMPONENT = "core.peerForwarder";
    private static final String BUFFER_ID_FORMAT = "%s.%s";
    private static final String BUFFER_USAGE_METRIC = "bufferUsage";

    private final int bufferSize;
    private final int batchSize;
    private final Semaphore capacitySemaphore;
    private final LinkedBlockingQueue<T> blockingQueue;
    private final AtomicDouble bufferUsage;
    private int recordsInFlight = 0;

    public PeerForwarderReceiveBuffer(final int bufferSize, final int batchSize, final String pipelineName, final String pluginId) {
        super(String.format(BUFFER_ID_FORMAT, pipelineName, pluginId), CORE_PEER_FORWARDER_COMPONENT);
        this.bufferSize = bufferSize;
        this.batchSize = batchSize;
        this.blockingQueue = new LinkedBlockingQueue<>(bufferSize);
        this.capacitySemaphore = new Semaphore(bufferSize);

        bufferUsage = pluginMetrics.gauge(BUFFER_USAGE_METRIC, new AtomicDouble());
    }

    @Override
    public void doWrite(final T record, final int timeoutInMillis) throws TimeoutException {
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException("Peer forwarder buffer is full, timed out waiting for a slot");
            }
            blockingQueue.offer(record);
        } catch (InterruptedException ex) {
            LOG.error("Peer forwarder buffer is full, interrupted while waiting to write the record", ex);
            throw new TimeoutException("Peer forwarder buffer is full, timed out waiting for a slot");
        }
    }

    @Override
    public void doWriteAll(final Collection<T> records, final int timeoutInMillis) throws Exception {
        final int size = records.size();
        if (size > bufferSize) {
            throw new SizeOverflowException(format("Peer forwarder buffer capacity too small for the number of records: %d", size));
        }
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(size, timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException(
                        format("Peer forwarder buffer does not have enough capacity left for the number of records: %d, " +
                                "timed out waiting for slots.", size));
            }
            blockingQueue.addAll(records);
        } catch (InterruptedException ex) {
            LOG.error("Peer forwarder buffer does not have enough capacity left for the number of records: {}, " +
                    "interrupted while waiting to write the records", size, ex);
            throw new TimeoutException(
                    format("Peer forwarder buffer does not have enough capacity left for the number of records: %d, " +
                            "timed out waiting for slots.", size));
        }
    }

    // TODO - consolidate duplicate logic in BlockingBuffer
    @Override
    public Map.Entry<Collection<T>, CheckpointState> doRead(final int timeoutInMillis) {
        final List<T> records = new ArrayList<>(batchSize);
        int recordsRead = 0;

        if (timeoutInMillis == 0) {
            final T record = pollForBufferEntry(5, TimeUnit.MILLISECONDS);
            if (record != null) { //record can be null, avoiding adding nulls
                records.add(record);
                recordsRead++;
            }

            recordsRead += blockingQueue.drainTo(records, batchSize - 1);
        } else {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutInMillis && records.size() < batchSize) {
                final T record = pollForBufferEntry(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (record != null) { //record can be null, avoiding adding nulls
                    records.add(record);
                    recordsRead++;
                }

                if (recordsRead < batchSize) {
                    recordsRead += blockingQueue.drainTo(records, batchSize - recordsRead);
                }
            }
        }

        final CheckpointState checkpointState = new CheckpointState(recordsRead);
        recordsInFlight += checkpointState.getNumRecordsToBeChecked();
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    private T pollForBufferEntry(final int timeoutValue, final TimeUnit timeoutUnit) {
        try {
            return blockingQueue.poll(timeoutValue, timeoutUnit);
        } catch (InterruptedException e) {
            LOG.info("Peer forwarder buffer - Interrupt received while reading from buffer");
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void postProcess(final Long recordsInBuffer) {
        // adding bounds to address race conditions and reporting negative buffer usage
        final Double nonNegativeTotalRecords = recordsInBuffer.doubleValue() < 0 ? 0 : recordsInBuffer.doubleValue();
        final Double boundedTotalRecords = nonNegativeTotalRecords > bufferSize ? bufferSize : nonNegativeTotalRecords;
        final Double usage = boundedTotalRecords / bufferSize * 100;
        bufferUsage.set(usage);
    }

    @Override
    public void doCheckpoint(final CheckpointState checkpointState) {
        final int numCheckedRecords = checkpointState.getNumRecordsToBeChecked();
        capacitySemaphore.release(numCheckedRecords);
        recordsInFlight -= checkpointState.getNumRecordsToBeChecked();
    }

    @Override
    public boolean isEmpty() {
        return blockingQueue.isEmpty() && recordsInFlight == 0;
    }
}
