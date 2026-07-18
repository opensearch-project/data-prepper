/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeedPollerTest {

    @Mock
    private RssReader rssReader;
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private Counter pollsFailedCounter;
    @Mock
    private Counter itemsIngestedCounter;

    private final String url = "https://example.com/feed?token=secret";
    private FeedPoller poller;

    @BeforeEach
    void setUp() {
        poller = new FeedPoller(rssReader, url, "tech", buffer, new RssItemMapper(),
                new SeenItemTracker(1000), pollsFailedCounter, itemsIngestedCounter,
                new Backoff(Duration.ofMillis(1), Duration.ofMillis(1), 2.0, 0.0), 500);
    }

    private Item item(final String guid) {
        return RssTestFixtures.item(null, "https://example.com/" + guid, null, null, guid);
    }

    @Test
    void writes_new_items_to_buffer() throws Exception {
        when(rssReader.read(url)).thenReturn(Stream.of(item("a"), item("b")));
        poller.run();
        verify(buffer, times(1)).writeAll(anyCollection(), anyInt());
        verify(itemsIngestedCounter).increment(2.0);
    }

    @Test
    void filters_already_seen_items_across_polls() throws Exception {
        when(rssReader.read(url))
                .thenReturn(Stream.of(item("a"), item("b")))
                .thenReturn(Stream.of(item("a"), item("b"), item("c")));
        poller.run(); // writes a, b
        poller.run(); // writes only c
        verify(buffer, times(2)).writeAll(anyCollection(), anyInt());
        verify(itemsIngestedCounter).increment(2.0);
        verify(itemsIngestedCounter).increment(1.0);
    }

    @Test
    void fetch_exception_is_isolated_and_not_rethrown() throws Exception {
        when(rssReader.read(url)).thenThrow(IOException.class);
        poller.run(); // must not throw
        verify(pollsFailedCounter).increment();
        verifyNoInteractions(buffer);
    }

    @Test
    void empty_feed_writes_nothing() throws Exception {
        when(rssReader.read(url)).thenReturn(Stream.empty());
        poller.run();
        verify(buffer, never()).writeAll(anyCollection(), anyInt());
    }

    @Test
    void write_timeout_leaves_items_unseen_for_retry() throws Exception {
        when(rssReader.read(url))
                .thenReturn(Stream.of(item("a")))
                .thenReturn(Stream.of(item("a")));
        doThrow(new RuntimeException("timeout")).when(buffer).writeAll(anyCollection(), anyInt());
        poller.run(); // write fails; "a" must not be marked seen
        poller.run(); // "a" is retried
        verify(buffer, times(2)).writeAll(anyCollection(), anyInt());
        verify(pollsFailedCounter, times(2)).increment();
    }
}
