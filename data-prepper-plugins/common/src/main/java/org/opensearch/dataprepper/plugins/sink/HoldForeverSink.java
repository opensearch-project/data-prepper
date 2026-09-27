/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A sink for testing purposes. It will hold data forever, which can be useful for
 * testing situations where data does not depart from Data Prepper.
 */
@DataPrepperPlugin(name = "hold_forever", pluginType = Sink.class, pluginConfigurationType = HoldForeverSink.HoldForeverSinkConfig.class)
public class HoldForeverSink implements Sink<Record<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(HoldForeverSink.class);

    private final AtomicLong recordsHeld = new AtomicLong(0);
    private final HoldForeverSinkConfig holdForeverSinkConfig;
    private final ReentrantLock lock;
    private Instant nextOutputTime;

    @DataPrepperPluginConstructor
    public HoldForeverSink(final HoldForeverSinkConfig holdForeverSinkConfig) {
        this.holdForeverSinkConfig = holdForeverSinkConfig;
        nextOutputTime = calculateNextOutputTime();
        lock = new ReentrantLock(true);
        LOG.warn("You are using the hold_forever sink which will not release events for acknowledgments. This is intended for testing, debugging, and experimenting only.");
    }

    @Override
    public void output(final Collection<Record<?>> records) {
        final long recordsHeldNow = recordsHeld.addAndGet(records.size());

        final boolean logThisIteration;
        lock.lock();
        try {
            if (Instant.now().isAfter(nextOutputTime)) {
                logThisIteration = true;
                nextOutputTime = calculateNextOutputTime();
            } else {
                logThisIteration = false;
            }
        } finally {
            lock.unlock();
        }

        if(logThisIteration) {
            LOG.info("Hold forever has {} records", recordsHeldNow);
        }
    }

    private Instant calculateNextOutputTime() {
        return Instant.now().plus(holdForeverSinkConfig.getOutputFrequency());
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void initialize() {

    }

    @Override
    public boolean isReady() {
        return true;
    }

    public static class HoldForeverSinkConfig {
        @JsonProperty("output_frequency")
        @JsonPropertyDescription("This sink will log how many records it is holding. This determines the frequency of the logging.")
        private Duration outputFrequency = Duration.of(2, ChronoUnit.MINUTES);

        public Duration getOutputFrequency() {
            return outputFrequency;
        }
    }
}
