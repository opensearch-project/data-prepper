/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;

import java.util.List;
import java.util.Objects;

/**
 *  implements the CloudwatchMetricsWorker to read and metrics data message and push to buffer.
 */
/**
 *  An implementation of cloudwatch metrics source  worker class to write the metric to Buffer
 */
public class CloudwatchMetricsWorker {

    private static final Logger LOG = LoggerFactory.getLogger(CloudwatchMetricsWorker.class);

    /**
     * Helps to write metrics data to buffer and to send end to end acknowledgements after successful processing
     * @param metricsData metricsData
     * @param bufferAccumulator bufferAccumulator
     * @param acknowledgementSet acknowledgementSet
     */
    public void writeToBuffer(final List<MetricDataResult> metricsData,
                              final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final AcknowledgementSet acknowledgementSet) {
        metricsData.forEach(message -> {
            final Record<Event> eventRecord = new Record<Event>(JacksonEvent.fromMessage(message.toString()));
            try {
                bufferAccumulator.add(eventRecord);
            } catch (Exception ex) {
                LOG.error("Exception while adding record events {0}", ex);
            }
            if(Objects.nonNull(acknowledgementSet)){
                acknowledgementSet.add(eventRecord.getData());
            }
        });
        try {
            bufferAccumulator.flush();
        } catch (final Exception ex) {
            LOG.error("Exception while flushing record events to buffer {0}", ex);
        }
    }
}
