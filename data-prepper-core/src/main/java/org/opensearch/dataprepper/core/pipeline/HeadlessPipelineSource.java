/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class HeadlessPipelineSource implements Source<Record<Event>>, HeadlessPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(HeadlessPipelineSource.class);
    private static final String NUMBER_OF_SUCCESSFUL_EVENTS_COUNTER = "numberOfEventsSuccessful";
    private static final String NUMBER_OF_FAILED_EVENTS_COUNTER = "numberOfEventsFailed";
    public static final int DEFAULT_WRITE_TIMEOUT = Integer.MAX_VALUE;
    private Buffer buffer;
    private AtomicBoolean isStopRequested;
    private PluginMetrics pluginMetrics;
    private final Counter numberOfEventsSuccessful;
    private final Counter numberOfEventsFailed;
    private boolean acknowledgementsEnabled;

    public HeadlessPipelineSource(final String componentName, final String scope) {
        pluginMetrics = PluginMetrics.fromNames(componentName, scope);
        isStopRequested = new AtomicBoolean(false);
        numberOfEventsSuccessful = pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_EVENTS_COUNTER);
        numberOfEventsFailed = pluginMetrics.counter(NUMBER_OF_FAILED_EVENTS_COUNTER);
        acknowledgementsEnabled = false;
    }

    @Override
    public void start(Buffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void stop() {
        isStopRequested.set(true);
    }

    @Override
    public void setAcknowledgementsEnabled(final boolean acknowledgementsEnabled) {
        this.acknowledgementsEnabled = acknowledgementsEnabled;
    }
    
    @VisibleForTesting
    public boolean getAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @VisibleForTesting
    long getNumberOfSuccessfulEvents() {
        return (long)numberOfEventsSuccessful.count();
    }

    @VisibleForTesting
    long getNumberOfFailedEvents() {
        return (long)numberOfEventsFailed.count();
    }

    @Override
    public void sendEvents(Collection<Record<Event>> records) {
        while (true) {
            try {
                buffer.writeAll(records, DEFAULT_WRITE_TIMEOUT);
                numberOfEventsSuccessful.increment(records.size());
                break;
            } catch (Exception e) {
                LOG.error(NOISY, "Failed to write to failure pipeline.", e);
                if (acknowledgementsEnabled) {
                    /* If acknowledgements enabled, better to retry here than retrying from the beginning */
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                    }
                } else {
                    numberOfEventsFailed.increment(records.size());
                    for (final Record record: records) {
                        ((Event)record.getData()).getEventHandle().release(false);
                    }
                    break;
                }
            }
        }
    }
}
