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

import com.apptasticsoftware.rssreader.RssReader;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.rss.config.BasicAuthConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedConfig;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@DataPrepperPlugin(name = "rss", pluginType = Source.class, pluginConfigurationType = FeedSourceConfig.class)
public class FeedSource implements Source<Record<Event>> {

    static final int BUFFER_TIMEOUT_MILLIS = 1000;
    static final int SEEN_CACHE_SIZE = 10_000;
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration BACKOFF_BASE = Duration.ofSeconds(30);
    private static final Duration BACKOFF_MAX = Duration.ofMinutes(10);
    private static final double BACKOFF_RATE = 2.0;
    private static final double BACKOFF_JITTER = 0.2;

    private final PluginMetrics pluginMetrics;
    private final FeedSourceConfig config;
    private final List<FeedPoller> pollers = new ArrayList<>();
    private ScheduledExecutorService executor;

    @DataPrepperPluginConstructor
    public FeedSource(final PluginMetrics pluginMetrics, final FeedSourceConfig config) {
        this.pluginMetrics = pluginMetrics;
        this.config = config;
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        final int feedCount = config.getFeeds().size();
        final int workers = workerCount(config.getWorkers(), feedCount);
        executor = Executors.newScheduledThreadPool(workers);
        final RssItemMapper mapper = new RssItemMapper();
        // Spread the feeds' first polls evenly across a window bounded by the
        // smallest polling interval, so all feeds do not fetch at once on startup
        // (thundering herd) and stay decorrelated afterward (each repeats at its
        // own interval). The window is capped at the smallest interval so no feed's
        // first poll is delayed beyond its own polling period.
        final long staggerStepMillis = smallestPollingIntervalMillis() / feedCount;
        int index = 0;
        for (final Map.Entry<String, FeedConfig> entry : config.getFeeds().entrySet()) {
            final String feedName = entry.getKey();
            final FeedConfig feed = entry.getValue();
            final Counter pollsFailed = pluginMetrics.counter("feedPollsFailed." + feedName);
            final Counter itemsIngested = pluginMetrics.counter("itemsIngested." + feedName);
            final Backoff backoff = new Backoff(BACKOFF_BASE, BACKOFF_MAX, BACKOFF_RATE, BACKOFF_JITTER);
            final long periodMillis = config.resolvePollingFrequency(feed).toMillis();
            final FeedPoller poller = new FeedPoller(buildReader(feed), feed.getUrl(), feedName,
                    buffer, mapper, new SeenItemTracker(SEEN_CACHE_SIZE), pollsFailed, itemsIngested,
                    backoff, BUFFER_TIMEOUT_MILLIS, executor, periodMillis);
            pollers.add(poller);
            // One-shot initial schedule; each poll re-schedules itself. Using
            // schedule (not scheduleAtFixedRate) with a self-rescheduling task
            // guarantees an inter-poll gap and keeps backoff off the pool threads.
            executor.schedule(poller, index * staggerStepMillis, TimeUnit.MILLISECONDS);
            index++;
        }
    }

    private long smallestPollingIntervalMillis() {
        return config.getFeeds().values().stream()
                .mapToLong(feed -> config.resolvePollingFrequency(feed).toMillis())
                .min()
                .orElse(config.getPollingFrequency().toMillis());
    }

    @Override
    public void stop() {
        pollers.forEach(FeedPoller::stop);
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private RssReader buildReader(final FeedConfig feed) {
        final RssReader rssReader = new RssReader();
        if (feed.getAuthentication() != null && feed.getAuthentication().getBasic() != null) {
            final BasicAuthConfig basic = feed.getAuthentication().getBasic();
            final String token = Base64.getEncoder().encodeToString(
                    (basic.getUsername() + ":" + basic.getPassword()).getBytes(StandardCharsets.UTF_8));
            rssReader.addHeader("Authorization", "Basic " + token);
        }
        return rssReader;
    }

    static int workerCount(final int configuredWorkers, final int feedCount) {
        return Math.max(1, Math.min(configuredWorkers, feedCount));
    }
}
