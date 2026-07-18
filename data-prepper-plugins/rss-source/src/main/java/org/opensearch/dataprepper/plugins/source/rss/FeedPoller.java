/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Polls a single feed on each scheduled invocation: fetch, map to events, filter
 * out already-seen items, and write the new ones to the buffer. Never propagates
 * an exception (which would suppress future scheduled runs); instead it logs,
 * increments a failure counter, and backs off.
 */
class FeedPoller implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(FeedPoller.class);

    private final RssReader rssReader;
    private final String url;
    private final String name;
    private final Buffer<Record<Event>> buffer;
    private final RssItemMapper mapper;
    private final SeenItemTracker tracker;
    private final Counter pollsFailedCounter;
    private final Counter itemsIngestedCounter;
    private final Backoff backoff;
    private final int bufferTimeoutMillis;

    private int consecutiveFailures = 0;

    FeedPoller(final RssReader rssReader, final String url, final String name,
               final Buffer<Record<Event>> buffer, final RssItemMapper mapper,
               final SeenItemTracker tracker, final Counter pollsFailedCounter,
               final Counter itemsIngestedCounter, final Backoff backoff,
               final int bufferTimeoutMillis) {
        this.rssReader = rssReader;
        this.url = url;
        this.name = name;
        this.buffer = buffer;
        this.mapper = mapper;
        this.tracker = tracker;
        this.pollsFailedCounter = pollsFailedCounter;
        this.itemsIngestedCounter = itemsIngestedCounter;
        this.backoff = backoff;
        this.bufferTimeoutMillis = bufferTimeoutMillis;
    }

    @Override
    public void run() {
        try {
            final List<Item> items = rssReader.read(url).collect(Collectors.toList());
            final List<Item> candidates = new ArrayList<>();
            final List<Record<Event>> toWrite = new ArrayList<>();
            for (final Item item : items) {
                if (!tracker.contains(mapper.dedupKey(item))) {
                    candidates.add(item);
                    toWrite.add(mapper.map(item, url, name));
                }
            }
            if (!toWrite.isEmpty()) {
                buffer.writeAll(toWrite, bufferTimeoutMillis);
                // Commit dedup keys only after a successful write, so a failed
                // write leaves items eligible for retry on the next poll.
                for (final Item item : candidates) {
                    tracker.addIfNew(mapper.dedupKey(item));
                }
                itemsIngestedCounter.increment(toWrite.size());
            }
            consecutiveFailures = 0;
        } catch (final Throwable t) {
            consecutiveFailures++;
            pollsFailedCounter.increment();
            LOG.warn("Feed poll failed ({} consecutive) for {}: {}",
                    consecutiveFailures, FeedUrls.redact(url), t.getMessage());
            sleep(backoff.nextDelayMillis(consecutiveFailures));
        }
    }

    private void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
