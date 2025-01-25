package org.opensearch.dataprepper.plugins.lambda.common.accumulator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferSynchronized;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class InMemoryBufferSynchronizedTest {
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    void testAddRecordAndGetRecords() {
        InMemoryBufferSynchronized buffer = new InMemoryBufferSynchronized("testKey");

        // Initially empty
        assertEquals(0, buffer.getEventCount());
        assertTrue(buffer.getRecords().isEmpty());
        assertEquals(0, buffer.getSize());

        // Add a record
        Event event = createSimpleEvent("hello", 123);
        buffer.addRecord(new Record<>(event));

        assertEquals(1, buffer.getEventCount());
        assertEquals(1, buffer.getRecords().size());
        assertTrue(buffer.getSize() > 0, "ByteArrayOutputStream should have some bytes after writing an event");
    }

    @Test
    void testGetRequestPayloadWhenEmptyReturnsNull() {
        InMemoryBufferSynchronized buffer = new InMemoryBufferSynchronized("testKey");
        // No records added => eventCount=0
        InvokeRequest request = buffer.getRequestPayload("someFunction", "RequestResponse");
        assertNull(request, "Expected null request if no events are in the buffer");
    }

    @Test
    void testGetRequestPayloadNonEmpty() {
        InMemoryBufferSynchronized buffer = new InMemoryBufferSynchronized("testKey");
        buffer.addRecord(new Record<>(createSimpleEvent("k1", 111)));
        buffer.addRecord(new Record<>(createSimpleEvent("k2", 222)));

        // Now we should have 2 events
        assertEquals(2, buffer.getEventCount());

        // getRequestPayload => closes JSON, returns an InvokeRequest
        InvokeRequest request = buffer.getRequestPayload("testFunction", "RequestResponse");
        assertNotNull(request);
        // Should not be null after we finalize
        SdkBytes payload = request.payload();
        assertNotNull(payload);
        // The payload should contain some JSON array with 2 items
        String payloadString = payload.asUtf8String();
        assertTrue(payloadString.contains("\"k1\":\"111\""), "Expected 'k1' field in JSON");
        assertTrue(payloadString.contains("\"k2\":\"222\""), "Expected 'k2' field in JSON");

        // Also, verify the payloadRequestSize is set
        Long requestSize = buffer.getPayloadRequestSize();
        assertNotNull(requestSize);
        assertTrue(requestSize > 0, "Expected a non-zero payload request size");
    }

    @Test
    void testConcurrentAddRecords() throws InterruptedException {
        InMemoryBufferSynchronized buffer = new InMemoryBufferSynchronized("testKey");

        int numThreads = 5;
        int recordsPerThread = 10;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        // Each thread adds 10 records => total 50
        for (int t = 0; t < numThreads; t++) {
            pool.submit(() -> {
                for (int i = 0; i < recordsPerThread; i++) {
                    buffer.addRecord(new Record<>(createSimpleEvent("thread", i)));
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS),
                "Threads did not finish in time");

        // Should now have 50 records
        assertEquals(numThreads * recordsPerThread, buffer.getEventCount());
        assertEquals(numThreads * recordsPerThread, buffer.getRecords().size());

        // ensure we get a JSON array with 50 items
        InvokeRequest request = buffer.getRequestPayload("threadFunction", "RequestResponse");
        String payloadStr = request.payload().asUtf8String();
        // Just check if it has multiple items
        long countOfThread = countOccurrences(payloadStr, "\"thread\":\"");
        assertTrue(countOfThread >= numThreads,
                "Expected multiple 'thread' fields in the JSON payload, found " + countOfThread);
    }

    // Utility to create a simple test event
    private Event createSimpleEvent(String key, int value) {
        // This is just one possible way to create a test Event
        return JacksonEvent.builder()
                .withData(Collections.singletonMap(key, String.valueOf(value)))
                .withEventType("TEST")
                .build();
    }

    // Utility to count occurrences of a substring
    private static long countOccurrences(String haystack, String needle) {
        long count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
