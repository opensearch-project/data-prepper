/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.confluence.rest.ConfluenceRestClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

@ExtendWith(MockitoExtension.class)
public class ConfluenceIteratorTest {

    @Mock
    private ConfluenceRestClient confluenceRestClient;
    private ConfluenceService confluenceService;
    @Mock
    private ConfluenceSourceConfig confluenceSourceConfig;
    private ConfluenceIterator confluenceIterator;
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("confluenceService", "aws");
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    @BeforeEach
    void setUp() {
        confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics));
    }

    public ConfluenceIterator createObjectUnderTest() {
        return new ConfluenceIterator(confluenceService, executorServiceProvider, confluenceSourceConfig);
    }

    @Test
    void testInitialization() {
        confluenceIterator = createObjectUnderTest();
        assertNotNull(confluenceIterator);
        confluenceIterator.initialize(Instant.ofEpochSecond(0));
        assertFalse(confluenceIterator.hasNext());
    }

    @Test
    void sleepInterruptionTest() {
        confluenceIterator = createObjectUnderTest();
        confluenceIterator.initialize(Instant.ofEpochSecond(0));

        Thread testThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> {
                try {
                    confluenceIterator.hasNext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });

        testThread.start();
        testThread.interrupt();
    }


    @Test
    void testStartCrawlerThreads() {
        confluenceIterator = createObjectUnderTest();
        confluenceIterator.initialize(Instant.ofEpochSecond(0));
        confluenceIterator.hasNext();
        confluenceIterator.hasNext();
        assertEquals(1, confluenceIterator.showFutureList().size());
    }


}
