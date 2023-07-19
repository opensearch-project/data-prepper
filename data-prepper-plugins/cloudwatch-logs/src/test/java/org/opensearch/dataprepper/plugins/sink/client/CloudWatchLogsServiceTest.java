package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.push_condition.CloudWatchLogsLimits;

import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.Mockito.*;

public class CloudWatchLogsServiceTest {
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher dispatcher;
    private static final int messageKeyByteSize = 14;
    private volatile int testCounter;

    @BeforeEach
    void setUp() {
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        thresholdConfig = new ThresholdConfig(); //Class can stay as is.
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(), thresholdConfig.getLogSendInterval());

        inMemoryBufferFactory = new InMemoryBufferFactory();
        buffer = inMemoryBufferFactory.getBuffer();
        dispatcher = mock(CloudWatchLogsDispatcher.class);

        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsLimits, dispatcher);

        testCounter = 0;
    }

    Collection<Record<Event>> getSampleRecordsLess() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecords() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsLarge() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < (thresholdConfig.getBatchSize() * 4); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            final EventHandle mockEventHandle = mock(EventHandle.class);
            mockJacksonEvent.setEventHandle(mockEventHandle);
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    @Test
    void check_dispatcher_run_was_not_called() {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLess());
        verify(dispatcher, never()).run();
    }

    @Test
    void check_dispatcher_run_was_called_test() {
        cloudWatchLogsService.processLogEvents(getSampleRecords());
        verify(dispatcher, atLeastOnce()).run();
    }

    @Test
    void check_dispatcher_run_called_heavy_load() {
        cloudWatchLogsService.processLogEvents(getSampleRecordsLarge());
        verify(dispatcher, atLeast(4)).run();
    }

    //TODO: Add multithreaded testing to ensure that the proper methods (run) gets called.
}
