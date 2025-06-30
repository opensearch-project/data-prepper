package org.opensearch.dataprepper.core.pipeline.buffer;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.core.pipeline.PipelineRunner;
import org.opensearch.dataprepper.core.pipeline.SupportsPipelineRunner;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "zero", pluginType = Buffer.class)
public class ZeroBuffer<T extends Record<?>> implements Buffer<T>, SupportsPipelineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ZeroBuffer.class);
    private static final String PLUGIN_COMPONENT_ID = "ZeroBuffer";
    static final CheckpointState EMPTY_CHECKPOINT = new CheckpointState(0);
    static final int DEFAULT_READ_SLEEP_MILLIS = 1000;
    private final PluginMetrics pluginMetrics;
    private final ThreadLocal<Collection<T>> threadLocalStore;
    private PipelineRunner pipelineRunner;
    @VisibleForTesting
    final String pipelineName;
    private final Counter writeRecordsCounter;
    private final Counter readRecordsCounter;

    @DataPrepperPluginConstructor
    public ZeroBuffer(PipelineDescription pipelineDescription) {
        this.pluginMetrics = PluginMetrics.fromNames(PLUGIN_COMPONENT_ID, pipelineDescription.getPipelineName());
        this.pipelineName = pipelineDescription.getPipelineName();
        this.threadLocalStore = new ThreadLocal<>();
        this.writeRecordsCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.readRecordsCounter = pluginMetrics.counter(MetricNames.RECORDS_READ);
    }

    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        if (record == null) {
            throw new NullPointerException("The write record cannot be null");
        }

        if (threadLocalStore.get() == null) {
            threadLocalStore.set(new ArrayList<>());
        }

        threadLocalStore.get().add(record);
        writeRecordsCounter.increment();

        getPipelineRunner().runAllProcessorsAndPublishToSinks();
    }

    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        if (records == null) {
            throw new NullPointerException("The write records cannot be null");
        }

        if (threadLocalStore.get() == null) {
            threadLocalStore.set(new ArrayList<>(records));
        } else {
            // Add the new records to the existing records
            threadLocalStore.get().addAll(records);
        }

        writeRecordsCounter.increment((double) records.size());
        getPipelineRunner().runAllProcessorsAndPublishToSinks();
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        if (threadLocalStore.get() == null) {
            try {
                Thread.sleep(DEFAULT_READ_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                LOG.debug("Thread interrupted while waiting for data in empty buffer, returning empty result");
            }
            return Map.entry(Collections.emptySet(), EMPTY_CHECKPOINT);
        }

        Collection<T> storedRecords = threadLocalStore.get();
        CheckpointState checkpointState = EMPTY_CHECKPOINT;
        if (storedRecords!= null && !storedRecords.isEmpty()) {
            checkpointState = new CheckpointState(storedRecords.size());
            threadLocalStore.remove();
            readRecordsCounter.increment((double) storedRecords.size());
        }

        return Map.entry(storedRecords, checkpointState);
    }

    @Override
    public void checkpoint(CheckpointState checkpointState) {}

    @Override
    public boolean isEmpty() {
        return (this.threadLocalStore.get() == null || this.threadLocalStore.get().isEmpty());
    }

    @Override
    public PipelineRunner getPipelineRunner() {
        return pipelineRunner;
    }

    @Override
    public void setPipelineRunner(PipelineRunner pipelineRunner) {
        this.pipelineRunner = pipelineRunner;
    }
}
