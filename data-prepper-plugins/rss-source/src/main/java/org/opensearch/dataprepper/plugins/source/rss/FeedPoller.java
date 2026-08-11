/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Polls a single feed. Each invocation fetches the feed, maps to events, filters
 * out already-seen items, and writes the new ones to the buffer, then re-schedules
 * itself for the next poll. This is a one-shot self-rescheduling task rather than a
 * fixed-rate task: on success it re-arms after the normal polling interval, and on
 * failure it re-arms after an exponential backoff delay. The backoff is applied by
 * delaying the next schedule, never by sleeping on the pool thread, so a failing
 * feed cannot block the threads that other feeds need to run.
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
    private final ScheduledExecutorService executor;
    private final long pollingIntervalMillis;

    private volatile boolean running = true;
    private int consecutiveFailures = 0;

    FeedPoller(final RssReader rssReader, final String url, final String name,
               final Buffer<Record<Event>> buffer, final RssItemMapper mapper,
               final SeenItemTracker tracker, final Counter pollsFailedCounter,
               final Counter itemsIngestedCounter, final Backoff backoff,
               final int bufferTimeoutMillis, final ScheduledExecutorService executor,
               final long pollingIntervalMillis) {
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
        this.executor = executor;
        this.pollingIntervalMillis = pollingIntervalMillis;
    }

    @Override
    public void run() {
        if (!running) {
            return;
        }
        long nextDelayMillis = pollingIntervalMillis;
        try {
            poll();
            consecutiveFailures = 0;
        } catch (final Throwable t) {
            consecutiveFailures++;
            pollsFailedCounter.increment();
            LOG.warn("Feed poll failed ({} consecutive) for {}: {}",
                    consecutiveFailures, FeedUrls.redact(url), t.getMessage());
            nextDelayMillis = backoff.nextDelayMillis(consecutiveFailures);
        } finally {
            reschedule(nextDelayMillis);
        }
    }

    private void poll() throws Exception {
        final List<Item> items = rssReader.read(url).collect(Collectors.toList());
        final List<String> newKeys = new ArrayList<>();
        final List<Record<Event>> toWrite = new ArrayList<>();
        final Set<String> batchKeys = new HashSet<>();
        for (final Item item : items) {
            final String key = mapper.dedupKey(item);
            // Skip items already ingested (persistent tracker) and duplicates
            // within this same fetch (batchKeys).
            if (tracker.contains(key) || !batchKeys.add(key)) {
                continue;
            }
            newKeys.add(key);
            toWrite.add(mapper.map(item, url, name));
        }
        if (!toWrite.isEmpty()) {
            buffer.writeAll(toWrite, bufferTimeoutMillis);
            // Commit dedup keys only after a successful write, so a failed write
            // leaves items eligible for retry on the next poll.
            newKeys.forEach(tracker::addIfNew);
            itemsIngestedCounter.increment(toWrite.size());
        }
    }

    private void reschedule(final long delayMillis) {
        if (!running) {
            return;
        }
        try {
            executor.schedule(this, delayMillis, TimeUnit.MILLISECONDS);
        } catch (final RejectedExecutionException e) {
            // Executor is shutting down; stop rescheduling.
        }
    }

    void stop() {
        running = false;
    }
}
