/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.SinkForwardRecordsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

@DataPrepperPlugin(name = "file", pluginType = Sink.class, pluginConfigurationType = FileSinkConfig.class)
public class FileSink implements Sink<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    public static final String FILE_PATH = "path";

    private final String outputFilePath;
    private BufferedWriter writer;
    private final ReentrantLock lock;
    private boolean isStopRequested;
    private boolean initialized;
    private final String tagsTargetKey;
    private final SinkContext sinkContext;

    private final boolean appendMode;

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link FileSink} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param fileSinkConfig The file sink configuration
     * @param sinkContext sink context
     */
    @DataPrepperPluginConstructor
    public FileSink(final FileSinkConfig fileSinkConfig, final SinkContext sinkContext) {
        this.outputFilePath = fileSinkConfig.getPath();
        isStopRequested = false;
        this.appendMode = fileSinkConfig.getAppendMode();
        initialized = false;
        lock = new ReentrantLock(true);
        tagsTargetKey = Objects.nonNull(sinkContext) ? sinkContext.getTagsTargetKey() : null;
        this.sinkContext = sinkContext;
    }

    @Override
    public void output(final Collection<Record<Object>> records) {
        SinkForwardRecordsContext sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        lock.lock();
        Collection<Record<Event>> events = new ArrayList<>();

        try {
            if (isStopRequested)
                return;

            for (final Record<Object> record : records) {
                if (record.getData() instanceof Event) {
                    Event event = (Event)record.getData();
                    sinkForwardRecordsContext.addRecord(new Record<>(event));
                }

                try {
                    checkTypeAndWriteObject(record.getData(), writer);
                } catch (final IOException ex) {
                    throw new RuntimeException(format("Encountered exception writing to file %s", outputFilePath), ex);
                }
            }

            sinkContext.forwardRecords(sinkForwardRecordsContext, null, null);
            try {
                writer.flush();
            } catch (final IOException ex) {
                LOG.warn("Failed to flush for file {}", outputFilePath, ex);
            }
        } finally {
            lock.unlock();
        }
    }

    // Temporary function to support both trace and log ingestion pipelines.
    // TODO: This function should be removed with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    private void checkTypeAndWriteObject(final Object object, final BufferedWriter writer) throws IOException {
        if (object instanceof Event) {
            String output = ((Event)object).jsonBuilder().includeTags(tagsTargetKey).toJsonString();
            writer.write(output);
            writer.newLine();
            EventHandle eventHandle = ((Event)object).getEventHandle();
            if (eventHandle != null) {
                eventHandle.release(true);
            }
        } else {
            writer.write(object.toString());
            writer.newLine();
        }
    }

    @Override
    public void shutdown() {
        isStopRequested = true;
        lock.lock();
        try {
            writer.close();
        } catch (final IOException ex) {
            LOG.error("Failed to close file {}.", outputFilePath, ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void initialize() {
        final StandardOpenOption[] openOptions = appendMode ?
                new StandardOpenOption[] {StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE} :
                new StandardOpenOption[] {};
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFilePath), StandardCharsets.UTF_8, openOptions);
        } catch (final IOException ex) {
            throw new RuntimeException(format("Encountered exception opening/creating file %s", outputFilePath), ex);
        }
        initialized = true;
    }

    @Override
    public boolean isReady() {
        return initialized;
    }
}
