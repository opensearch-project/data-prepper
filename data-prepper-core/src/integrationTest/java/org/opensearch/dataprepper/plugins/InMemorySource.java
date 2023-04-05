/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A Data Prepper source which can receive records via the {@link InMemorySourceAccessor}.
 */
@DataPrepperPlugin(name = "in_memory", pluginType = Source.class, pluginConfigurationType = InMemoryConfig.class)
public class InMemorySource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemorySource.class);

    private final String testingKey;
    private final InMemorySourceAccessor inMemorySourceAccessor;
    private boolean isStopped = false;
    private Thread runningThread;

    @DataPrepperPluginConstructor
    public InMemorySource(
            final InMemoryConfig inMemoryConfig,
            final InMemorySourceAccessor inMemorySourceAccessor) {
        testingKey = inMemoryConfig.getTestingKey();

        this.inMemorySourceAccessor = inMemorySourceAccessor;
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        runningThread = new Thread(new SourceRunner(buffer));

        runningThread.start();
    }

    @Override
    public void stop() {
        isStopped = true;
        runningThread.interrupt();
    }

    private class SourceRunner implements Runnable {
        private final Buffer<Record<Event>> buffer;

        SourceRunner(final Buffer<Record<Event>> buffer) {
            this.buffer = buffer;
        }

        @Override
        public void run() {
            while (!isStopped) {
                try {
                    final List<Record<Event>> records = inMemorySourceAccessor.read(testingKey);
                    if(!records.isEmpty()) {
                        buffer.writeAll(records, 200);
                    } else {
                        Thread.sleep(10);
                    }
                } catch (final Exception ex) {
                    LOG.error("Error during source loop.", ex);
                }
            }
        }
    }
}
