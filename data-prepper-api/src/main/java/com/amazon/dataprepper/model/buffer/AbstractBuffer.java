package com.amazon.dataprepper.model.buffer;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Abstract implementation of the Buffer interface to record boilerplate metrics
 */
public abstract class AbstractBuffer<T extends Record<?>> implements Buffer<T> {
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsWrittenCounter;
    private final Counter recordsInflightCounter;
    private final Counter recordsProcessedCounter;
    private final Counter writeTimeoutCounter;
    private final Timer writeTimer;
    private final Timer readTimer;

    public AbstractBuffer(final PluginSetting pluginSetting) {
        this(PluginMetrics.fromPluginSetting(pluginSetting));
    }

    public AbstractBuffer(final String bufferName, final String pipelineName) {
        this(PluginMetrics.fromNames(bufferName, pipelineName));
    }

    private AbstractBuffer(final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.recordsWrittenCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.recordsInflightCounter = pluginMetrics.counter(MetricNames.RECORDS_INFLIGHT);
        this.recordsProcessedCounter = pluginMetrics.counter(MetricNames.RECORDS_PROCESSED);
        this.writeTimeoutCounter = pluginMetrics.counter(MetricNames.WRITE_TIMEOUTS);
        this.writeTimer = pluginMetrics.timer(MetricNames.WRITE_TIME_ELAPSED);
        this.readTimer = pluginMetrics.timer(MetricNames.READ_TIME_ELAPSED);
    }

    /**
     * Records metrics for ingress, time elapsed, and timeouts, while calling the doWrite method
     * to perform the actual write
     * @param record the Record to add
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException
     */
    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        try {
            writeTimer.record(() -> {
                try {
                    doWrite(record, timeoutInMillis);
                } catch (TimeoutException e) {
                    writeTimeoutCounter.increment();
                    throw new RuntimeException(e);
                }
            });
            recordsWrittenCounter.increment();
        } catch (RuntimeException e) {
            if(e.getCause() instanceof TimeoutException) {
                throw (TimeoutException) e.getCause();
            } else  {
                throw e;
            }
        }
    }

    /**
     * Records egress and time elapsed metrics, while calling the doRead function to
     * do the actual read
     * @param timeoutInMillis how long to wait before giving up
     * @return Records collection and checkpoint state read from the buffer
     */
    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        final Map.Entry<Collection<T>, CheckpointState> readResult = readTimer.record(() -> doRead(timeoutInMillis));
        recordsInflightCounter.increment(readResult.getValue().getNumRecordsToBeChecked()*1.0);
        return readResult;
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        doCheckpoint(checkpointState);
        final int numRecordsToBeChecked = checkpointState.getNumRecordsToBeChecked();
        recordsInflightCounter.increment(-numRecordsToBeChecked);
        recordsProcessedCounter.increment(numRecordsToBeChecked);
    }

    /**
     * This method should implement the logic for writing to the  buffer
     * @param record Record to write to buffer
     * @param timeoutInMillis Timeout for write operation in millis
     * @throws TimeoutException
     */
    public abstract void doWrite(T record, int timeoutInMillis) throws TimeoutException;

    /**
     * This method should implement the logic for reading from the buffer
     * @param timeoutInMillis Timeout in millis
     * @return Records collection and checkpoint state read from the buffer
     */
    public abstract Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis);

    public abstract void doCheckpoint(CheckpointState checkpointState);
}
