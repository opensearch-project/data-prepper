/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Generates a random string every 500 milliseconds. Intended to be used for testing setups
 */
@DataPrepperPlugin(name = "random", pluginType = Source.class)
public class RandomStringSource implements Source<Record<Event>> {

    static final String MESSAGE_KEY = "message";
    static final String EVENT_TYPE = "event";
    private static final Logger LOG = LoggerFactory.getLogger(RandomStringSource.class);

    private volatile boolean stop = false;

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        while (!stop) {
            try {
                LOG.info("Writing to buffer");
                final Record<Event> record = generateRandomStringEventRecord();
                buffer.write(record, 500);
                Thread.sleep(500);
            } catch (final InterruptedException e) {
                LOG.error("Writing random string to buffer interrupted", e);
            } catch (final TimeoutException e) {
                LOG.error("Writing timed out", e);
            }
        }
    }

    @Override
    public void stop() {
        stop = true;
    }

    private Record<Event> generateRandomStringEventRecord() {
        final Map<String, Object> structuredLine = Map.of(MESSAGE_KEY, UUID.randomUUID().toString());
        return new Record<>(JacksonEvent
                .builder()
                .withEventType(EVENT_TYPE)
                .withData(structuredLine)
                .build());
    }
}
