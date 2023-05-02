/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

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
    private final AcknowledgementSetManager acknowledgementSetManager;
    private boolean enabledAcks;

    @DataPrepperPluginConstructor
    public InMemorySource(
            final InMemoryConfig inMemoryConfig,
            final EventFactory eventFactory,
            final AcknowledgementSetManager acknowledgementSetManager,
            final InMemorySourceAccessor inMemorySourceAccessor) {
        testingKey = inMemoryConfig.getTestingKey();

        this.inMemorySourceAccessor = inMemorySourceAccessor;
        this.inMemorySourceAccessor.setEventFactory(eventFactory);
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.enabledAcks = inMemoryConfig.getAcknowledgements();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (enabledAcks) {
            runningThread = new Thread(new AcknowledgementSourceRunner(acknowledgementSetManager, buffer));
        } else {
            runningThread = new Thread(new SourceRunner(buffer));
        }

        runningThread.start();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return this.enabledAcks;
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

        protected void writeToBuffer(final List<Record<Event>> records) throws Exception {
            if(!records.isEmpty()) {
                buffer.writeAll(records, 200);
            } else {
                Thread.sleep(10);
            }
        }

        @Override
        public void run() {
            while (!isStopped) {
                try {
                    final List<Record<Event>> records = inMemorySourceAccessor.read(testingKey);
                    writeToBuffer(records);
                } catch (final Exception ex) {
                    LOG.error("Error during source loop.", ex);
                }
            }
        }
    }

    private class AcknowledgementSourceRunner extends SourceRunner {
        private final AcknowledgementSetManager acknowledgementSetManager;
        private AtomicBoolean ackReceived;

        AcknowledgementSourceRunner(final AcknowledgementSetManager acknowledgementSetManager, final Buffer<Record<Event>> buffer) {
            super(buffer);
            this.acknowledgementSetManager = acknowledgementSetManager;
            this.ackReceived = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            while (!isStopped) {
                try {
                    final List<Record<Event>> records = inMemorySourceAccessor.read(testingKey);
                    AcknowledgementSet ackSet =
                            acknowledgementSetManager.create((result) ->
                                {
                                    inMemorySourceAccessor.setAckReceived(result);
                                },
                                Duration.ofSeconds(15));
                    records.stream().forEach((record) -> { ackSet.add(record.getData()); });
                    writeToBuffer(records);
                } catch (final Exception ex) {
                    LOG.error("Error during source loop.", ex);
                }
            }
        }
    }

}
