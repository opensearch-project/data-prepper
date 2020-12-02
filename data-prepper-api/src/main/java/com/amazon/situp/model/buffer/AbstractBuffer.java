package com.amazon.situp.model.buffer;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.metrics.MetricNames;
import com.amazon.situp.metrics.PluginMetrics;
import com.amazon.situp.model.record.Record;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Abstract implementation of the Buffer interface to record boilerplate metrics
 */
public abstract class AbstractBuffer<T extends Record<?>> implements Buffer<T> {
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsWrittenCounter;
    private final Counter recordsReadCounter;
    private final Counter writeTimeoutCounter;
    private final Timer writeTimer;
    private final Timer readTimer;

    public AbstractBuffer(final PluginSetting pluginSetting) {
        this.pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        this.recordsWrittenCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.recordsReadCounter = pluginMetrics.counter(MetricNames.RECORDS_READ);
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
     * @return Collection<Records> read
     */
    @Override
    public Collection<T> read(int timeoutInMillis) {
        Collection<T> records = readTimer.record(() -> doRead(timeoutInMillis));
        recordsReadCounter.increment(records.size()*1.0);
        return records;
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
     * @return Records read from the buffer
     */
    public abstract Collection<T> doRead(int timeoutInMillis);
}
