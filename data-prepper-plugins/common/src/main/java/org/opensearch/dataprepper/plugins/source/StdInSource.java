/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * A simple source which reads data from console each line at a time. It exits when it reads case insensitive "exit"
 * from console or if Pipeline notifies to stop.
 */
@DataPrepperPlugin(name = "stdin", pluginType = Source.class, pluginConfigurationType = StdInSourceConfig.class)
public class StdInSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(StdInSource.class);
    private static final String ATTRIBUTE_TIMEOUT = "write_timeout";
    private static final int WRITE_TIMEOUT = 5_000;
    private final Scanner reader;
    private final int writeTimeout;
    private final String pipelineName;
    private boolean isStopRequested;

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StdInSource} using an instance of {@link StdInSourceConfig}
     *
     * @param stdInSourceConfig The configuration instance for {@link StdInSource}
     * @param pipelineDescription The pipeline description which has access to pipeline Name
     */
    public StdInSource(final StdInSourceConfig stdInSourceConfig, final PipelineDescription pipelineDescription) {
        this(checkNotNull(stdInSourceConfig, "StdInSourceConfig cannot be null")
                        .getWriteTimeout(),
                pipelineDescription.getPipelineName());
    }

    public StdInSource(final int writeTimeout, final String pipelineName) {
        this.writeTimeout = writeTimeout;
        this.pipelineName = checkNotNull(pipelineName, "Pipeline name cannot be null");
        this.reader = new Scanner(System.in);
        isStopRequested = false;
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        checkNotNull(buffer, format("Pipeline [%s] - buffer cannot be null for source to start", pipelineName));
        String line = reader.nextLine();
        while (!"exit".equalsIgnoreCase(line) && !isStopRequested) {
            final Record<Event> record = convertLineIntoEventRecord(line);
            try {
                buffer.write(record, writeTimeout);
            } catch (TimeoutException ex) {
                LOG.error("Pipeline [{}] - Timed out writing to buffer; Will exit without further processing",
                        pipelineName, ex);
                throw new RuntimeException(format("Pipeline [%s] - Timed out writing to buffer", pipelineName), ex);
            }
            line = reader.nextLine();
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }

    private Record<Event> convertLineIntoEventRecord(final String line) {
        final Event event = JacksonEvent.fromMessage(line);
        return new Record<>(event);
    }
}