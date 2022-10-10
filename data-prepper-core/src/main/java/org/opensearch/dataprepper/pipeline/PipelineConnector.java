/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * PipelineConnector is a special type of Plugin which connects two pipelines acting both as Sink and Source.
 *
 * @param <T>
 */
public final class PipelineConnector<T extends Record<?>> implements Source<T>, Sink<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConnector.class);
    private static final int DEFAULT_WRITE_TIMEOUT = Integer.MAX_VALUE;
    private String sourcePipelineName; //name of the pipeline for which this connector acts as source
    private String sinkPipelineName; //name of the pipeline for which this connector acts as sink
    private Buffer<T> buffer;
    private AtomicBoolean isStopRequested;

    public PipelineConnector() {
        isStopRequested = new AtomicBoolean(false);
    }

    public PipelineConnector(final String sinkPipelineName) {
        this();
        this.sinkPipelineName = sinkPipelineName;
    }

    @Override
    public void start(final Buffer<T> buffer) {
        this.buffer = buffer;
    }

    @Override
    public void stop() {
        isStopRequested.set(true);
    }

    @Override
    public void output(final Collection<T> records) {
        if (buffer != null && !isStopRequested.get()) {
            for (T record : records) {
		if(record.getData() instanceof JacksonSpan) {
		    try {
		        final JacksonSpan span = (JacksonSpan)record.getData();
			JacksonSpan newSpanEvent = JacksonSpan.builder()
			          .withJsonData(span.toJsonString())
				  .withEventMetadata(span.getMetadata())
			          .build(); 
			record = (T) (new Record<>(newSpanEvent));
		    } catch (Exception ex) {
                        LOG.error("PipelineConnector [{}-{}]:  exception while duplicating the event [{}]",
                                sinkPipelineName, sourcePipelineName, ex);
                    }
		} else if(record.getData() instanceof Event) {
		    try {
		        final Event recordEvent = (Event)record.getData();
			Event newRecordEvent = JacksonEvent.builder() 
			          .withData(recordEvent.toMap()) 
				  .withEventMetadata(recordEvent.getMetadata()) 
			          .build(); 
			record = (T) (new Record<>(newRecordEvent));
		    } catch (Exception ex) {
                        LOG.error("PipelineConnector [{}-{}]:  exception while duplicating the event [{}]",
                                sinkPipelineName, sourcePipelineName, ex);
                    }
		}

                while (true) {
                    try {
                        buffer.write(record, DEFAULT_WRITE_TIMEOUT);
                        break;
                    } catch (TimeoutException ex) {
                        LOG.error("PipelineConnector [{}-{}]: Timed out writing to pipeline [{}]",
                                sinkPipelineName, sourcePipelineName, sourcePipelineName, ex);
                    }
                }

            }
        } else {
            LOG.error("PipelineConnector [{}-{}]: Pipeline [{}] is currently not initialized or has been halted",
                    sinkPipelineName, sourcePipelineName, sourcePipelineName);
            throw new RuntimeException(format("PipelineConnector [%s-%s]: Pipeline [%s] is not active, " +
                    "cannot proceed", sinkPipelineName, sourcePipelineName, sourcePipelineName));
        }
    }

    @Override
    public void shutdown() {
        //TODO: Cleanup resources
    }

    public void setSourcePipelineName(final String sourcePipelineName) {
        this.sourcePipelineName = sourcePipelineName;
    }

    public void setSinkPipelineName(final String sinkPipelineName) {
        this.sinkPipelineName = sinkPipelineName;
    }
}
