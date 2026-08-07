/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service that periodically scrapes Prometheus metrics endpoints and writes the parsed
 * metrics into the Data Prepper buffer.
 */
public class PrometheusScrapeService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusScrapeService.class);

    static final String SCRAPE_REQUESTS_METRIC = "scrapeRequests";
    static final String SCRAPE_SUCCESS_METRIC = "scrapeSuccess";
    static final String SCRAPE_FAILURE_METRIC = "scrapeFailure";
    static final String RECORDS_CREATED_METRIC = "recordsCreated";
    static final String SCRAPE_DURATION_METRIC = "scrapeDuration";

    private final PrometheusScrapeConfig config;
    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutMs;
    private final ScrapeTargetScraper scraper;
    private final TextExpositionParser parser;
    private final ScheduledExecutorService executor;
    private final Counter scrapeRequestsCounter;
    private final Counter scrapeSuccessCounter;
    private final Counter scrapeFailureCounter;
    private final Counter recordsCreatedCounter;
    private final Timer scrapeDurationTimer;

    public PrometheusScrapeService(final PrometheusScrapeConfig config,
                                   final Buffer<Record<Event>> buffer,
                                   final int bufferWriteTimeoutMs,
                                   final PluginMetrics pluginMetrics) {
        this.config = config;
        this.buffer = buffer;
        this.bufferWriteTimeoutMs = bufferWriteTimeoutMs;
        this.scraper = new ScrapeTargetScraper(config);
        this.parser = new TextExpositionParser(config.isFlattenLabels());
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "prometheus-scrape");
            t.setDaemon(true);
            return t;
        });
        this.scrapeRequestsCounter = pluginMetrics.counter(SCRAPE_REQUESTS_METRIC);
        this.scrapeSuccessCounter = pluginMetrics.counter(SCRAPE_SUCCESS_METRIC);
        this.scrapeFailureCounter = pluginMetrics.counter(SCRAPE_FAILURE_METRIC);
        this.recordsCreatedCounter = pluginMetrics.counter(RECORDS_CREATED_METRIC);
        this.scrapeDurationTimer = pluginMetrics.timer(SCRAPE_DURATION_METRIC);
    }

    public void start() {
        executor.scheduleAtFixedRate(this::scrapeAll, 0,
                config.getScrapeInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    void scrapeAll() {
        try {
            final List<ScrapeTargetConfig> targets = config.getTargets();
            for (final ScrapeTargetConfig target : targets) {
                try {
                    scrapeTarget(target.getUrl());
                } catch (final Exception e) {
                    LOG.error("Failed to scrape target {}", target.getUrl(), e);
                }
            }
        } catch (final Throwable t) {
            LOG.error("Unexpected error in scrapeAll", t);
        }
    }

    void scrapeTarget(final String url) {
        scrapeDurationTimer.record(() -> {
            scrapeRequestsCounter.increment();
            try {
                final String body = scraper.scrape(url);
                final List<Record<Event>> records = parser.parse(body);
                buffer.writeAll(records, bufferWriteTimeoutMs);
                recordsCreatedCounter.increment(records.size());
                scrapeSuccessCounter.increment();
            } catch (final Exception e) {
                scrapeFailureCounter.increment();
                LOG.error("Error scraping target {}", url, e);
            }
        });
    }

    public void stop() {
        try {
            executor.shutdown();
            try {
                final long timeoutMs = config.getScrapeTimeout().toMillis() + 1000;
                if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (final InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } finally {
            scraper.close();
        }
    }
}