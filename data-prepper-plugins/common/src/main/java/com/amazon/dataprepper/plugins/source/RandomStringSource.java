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
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
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
public class RandomStringSource implements Source<Record<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomStringSource.class);

    private ExecutorService executorService;
    private boolean stop = false;

    private void setExecutorService() {
        if(executorService == null || executorService.isShutdown()) {
            executorService = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setDaemon(false).setNameFormat("random-source-pool-%d").build()
            );
        }
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        setExecutorService();
        executorService.execute(() -> {
            while (!stop) {
                try {
                    LOG.info("Writing to buffer");
                    buffer.write(new Record<>(UUID.randomUUID().toString()), 500);
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
}
