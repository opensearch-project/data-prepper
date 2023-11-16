/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Generates a random string every 500 milliseconds. Intended to be used for testing setups
 */
@DataPrepperPlugin(name = "random", pluginType = Source.class, pluginConfigurationType = RandomStringSourceConfig.class)
public class RandomStringSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RandomStringSource.class);
    private static final int BUFFER_WAIT = 500;
    private final long waitTimeInMillis;

    private volatile boolean stop = false;
    private Thread thread;

    @DataPrepperPluginConstructor
    public RandomStringSource(final RandomStringSourceConfig config) {
        waitTimeInMillis = config.getWaitDelay().toMillis();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if(thread != null) {
            throw new IllegalStateException("This source has already started.");
        }

        thread = new Thread(() -> {
            while (!stop) {
                try {
                    LOG.debug("Writing to buffer");
                    final Record<Event> record = generateRandomStringEventRecord();
                    buffer.write(record, BUFFER_WAIT);
                    Thread.sleep(waitTimeInMillis);
                } catch (final InterruptedException e) {
                    break;
                } catch (final TimeoutException e) {
                    // Do nothing
                }
            }
        },
                "random-source");

        thread.setDaemon(false);
        thread.start();
    }

    @Override
    public void stop() {
        stop = true;
        try {
            thread.join(waitTimeInMillis + BUFFER_WAIT + 100);
        } catch (final InterruptedException e) {
            thread.interrupt();
        }
    }

    private Record<Event> generateRandomStringEventRecord() {
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        return new Record<>(event);
    }
}
