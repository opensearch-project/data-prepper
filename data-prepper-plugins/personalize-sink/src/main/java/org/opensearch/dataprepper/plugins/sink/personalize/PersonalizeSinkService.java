/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.personalize;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.PersonalizeSinkConfiguration;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class responsible for creating PersonalizeEventsClient object, check thresholds,
 * get new buffer and write records into buffer.
 */
class PersonalizeSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(PersonalizeSinkService.class);
    public static final String RECORDS_SUCCEEDED = "personalizeRecordsSucceeded";
    public static final String RECORDS_FAILED = "personalizeRecordsFailed";
    public static final String RECORDS_INVALID = "personalizeRecordsInvalid";
    public static final String REQUESTS_THROTTLED = "personalizeRequestsThrottled";
    public static final String REQUEST_LATENCY = "personalizeRequestLatency";

    private final PersonalizeSinkConfiguration personalizeSinkConfig;
    private final Lock reentrantLock;
    private final int maxRetries;
    private final Counter recordsSucceededCounter;
    private final Counter recordsFailedCounter;
    private final Counter recordsInvalidCounter;
    private final Counter requestsThrottledCounter;
    private final Timer requestLatencyTimer;

    /**
     * @param personalizeSinkConfig  personalize sink related configuration.
     * @param pluginMetrics metrics.
     */
    public PersonalizeSinkService(final PersonalizeSinkConfiguration personalizeSinkConfig,
                                  final PluginMetrics pluginMetrics) {
        this.personalizeSinkConfig = personalizeSinkConfig;
        reentrantLock = new ReentrantLock();

        maxRetries = personalizeSinkConfig.getMaxRetries();

        recordsSucceededCounter = pluginMetrics.counter(RECORDS_SUCCEEDED);
        recordsFailedCounter = pluginMetrics.counter(RECORDS_FAILED);
        recordsInvalidCounter = pluginMetrics.counter(RECORDS_INVALID);
        requestsThrottledCounter = pluginMetrics.counter(REQUESTS_THROTTLED);
        requestLatencyTimer = pluginMetrics.timer(REQUEST_LATENCY);
    }

    /**
     * @param records received records and add into buffer.
     */
    void output(Collection<Record<Event>> records) {
        LOG.trace("{} records received", records.size());
        return;
    }
}