/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.configuration.Office365ItemInfo;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@ExtendWith(MockitoExtension.class)
class Office365IteratorTest {

    @Mock
    private Office365Service office365Service;

    private Office365Iterator office365Iterator;

    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    @BeforeEach
    void setUp() {
        office365Iterator = new Office365Iterator(office365Service, executorServiceProvider);
    }

    @Test
    void testInitialization() {
        assertNotNull(office365Iterator);
        office365Iterator.initialize(Instant.EPOCH);
        assertFalse(office365Iterator.hasNext());
    }

    @Test
    void testEmptyQueueBehavior() {
        office365Iterator.initialize(Instant.EPOCH);
        office365Iterator.setCrawlerQWaitTimeMillis(1);

        assertFalse(office365Iterator.hasNext());
        assertThrows(NoSuchElementException.class, () -> office365Iterator.next());
    }

    @Test
    void testQueueOperations(){
        // Initialize iterator
        office365Iterator.initialize(Instant.EPOCH);
        office365Iterator.setCrawlerQWaitTimeMillis(1);

        // Initially queue should be empty
        assertFalse(office365Iterator.hasNext());

        // Create specific mock item to track
        Office365ItemInfo mockItem = createMockItemInfo();

        // Mock service to add our specific mock item
        doAnswer(invocation -> {
            Queue<ItemInfo> queue = invocation.getArgument(1);
            queue.add(mockItem);
            return null;
        }).when(office365Service).getOffice365Entities(any(Instant.class), any());

        // Manually trigger the crawler thread
        office365Iterator.startCrawlerThreads();

        // Now verify item is available
        assertTrue(office365Iterator.hasNext());

        // Verify we get the correct item
        assertEquals(mockItem, office365Iterator.next());

        // Verify queue is empty after retrieval
        assertFalse(office365Iterator.hasNext());
    }

    @Test
    void testInterruptedException() {
        office365Iterator.initialize(Instant.EPOCH);
        office365Iterator.setCrawlerQWaitTimeMillis(1);

        Thread.currentThread().interrupt();
        assertFalse(office365Iterator.hasNext());
        assertTrue(Thread.interrupted());
    }

    private Office365ItemInfo createMockItemInfo() {
        return Office365ItemInfo.builder()
                .itemId(UUID.randomUUID().toString())
                .eventTime(Instant.now())
                .partitionKey("test-partition")
                .lastModifiedAt(Instant.now())
                .build();
    }
}