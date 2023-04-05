/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Generates a random string every 500 milliseconds. Intended to be used for testing setups
 */
@DataPrepperPlugin(name = "random", pluginType = Source.class)
public class RandomStringSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomStringSource.class);

    private ExecutorService executorService;
    private volatile boolean stop = false;

    private void setExecutorService() {
        if(executorService == null || executorService.isShutdown()) {
            executorService = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setDaemon(false).setNameFormat("random-source-pool-%d").build()
            );
        }
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        setExecutorService();
        executorService.execute(() -> {
            while (!stop) {
                try {
                    LOG.debug("Writing to buffer");
                    final Record<Event> record = generateRandomStringEventRecord();
                    buffer.write(record, 500);
                    Thread.sleep(500);
                } catch (final InterruptedException e) {
                    break;
                } catch (final TimeoutException e) {
                    // Do nothing
                }
            }
        });
    }

    @Override
    public void stop() {
        stop = true;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            executorService.shutdownNow();
        }
    }

    private Record<Event> generateRandomStringEventRecord() {
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        return new Record<>(event);
    }
}
