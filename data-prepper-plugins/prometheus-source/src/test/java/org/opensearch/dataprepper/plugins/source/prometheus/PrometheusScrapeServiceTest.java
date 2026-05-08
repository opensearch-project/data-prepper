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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PrometheusScrapeServiceTest {

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PrometheusScrapeConfig config;

    @Mock
    private Counter scrapeRequestsCounter;

    @Mock
    private Counter scrapeSuccessCounter;

    @Mock
    private Counter scrapeFailureCounter;

    @Mock
    private Counter recordsCreatedCounter;

    @Mock
    private Timer scrapeDurationTimer;

    private static final int BUFFER_WRITE_TIMEOUT_MS = 5000;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(PrometheusScrapeService.SCRAPE_REQUESTS_METRIC)).thenReturn(scrapeRequestsCounter);
        when(pluginMetrics.counter(PrometheusScrapeService.SCRAPE_SUCCESS_METRIC)).thenReturn(scrapeSuccessCounter);
        when(pluginMetrics.counter(PrometheusScrapeService.SCRAPE_FAILURE_METRIC)).thenReturn(scrapeFailureCounter);
        when(pluginMetrics.counter(PrometheusScrapeService.RECORDS_CREATED_METRIC)).thenReturn(recordsCreatedCounter);
        when(pluginMetrics.timer(PrometheusScrapeService.SCRAPE_DURATION_METRIC)).thenReturn(scrapeDurationTimer);

        when(config.getScrapeInterval()).thenReturn(Duration.ofSeconds(15));
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        when(config.isFlattenLabels()).thenReturn(false);
        when(config.isInsecure()).thenReturn(false);
        when(config.getAuthentication()).thenReturn(null);
    }

    private PrometheusScrapeService createService() {
        return new PrometheusScrapeService(config, buffer, BUFFER_WRITE_TIMEOUT_MS, pluginMetrics);
    }

    @Test
    void testConstructorCreatesService() {
        final PrometheusScrapeService service = createService();
        assertThat(service, notNullValue());
    }

    @Test
    void testStartSchedulesExecution() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        setField(service, "executor", mockExecutor);

        service.start();

        verify(mockExecutor).scheduleAtFixedRate(
                any(Runnable.class),
                eq(0L),
                eq(15000L),
                eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void testScrapeAllIteratesAllTargets() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetConfig target1 = mock(ScrapeTargetConfig.class);
        when(target1.getUrl()).thenReturn("http://host1:9090/metrics");
        final ScrapeTargetConfig target2 = mock(ScrapeTargetConfig.class);
        when(target2.getUrl()).thenReturn("http://host2:9090/metrics");
        when(config.getTargets()).thenReturn(Arrays.asList(target1, target2));

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("test_gauge 1.0\n");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeAll();

        verify(scrapeRequestsCounter, times(2)).increment();
        verify(mockScraper).scrape("http://host1:9090/metrics");
        verify(mockScraper).scrape("http://host2:9090/metrics");
    }

    @Test
    void testScrapeAllContinuesOnTargetFailure() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetConfig target1 = mock(ScrapeTargetConfig.class);
        when(target1.getUrl()).thenReturn("http://failing-host:9090/metrics");
        final ScrapeTargetConfig target2 = mock(ScrapeTargetConfig.class);
        when(target2.getUrl()).thenReturn("http://working-host:9090/metrics");
        when(config.getTargets()).thenReturn(Arrays.asList(target1, target2));

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape("http://failing-host:9090/metrics"))
                .thenThrow(new RuntimeException("Connection refused"));
        when(mockScraper.scrape("http://working-host:9090/metrics"))
                .thenReturn("test_gauge 42.0\n");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeAll();

        verify(mockScraper).scrape("http://failing-host:9090/metrics");
        verify(mockScraper).scrape("http://working-host:9090/metrics");
        verify(scrapeRequestsCounter, times(2)).increment();
    }

    @Test
    void testScrapeTargetIncrementsSuccessOnSuccess() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("test_gauge 1.0\n");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(scrapeRequestsCounter).increment();
        verify(scrapeSuccessCounter).increment();
        verify(scrapeFailureCounter, never()).increment();
    }

    @Test
    void testScrapeTargetIncrementsFailureOnError() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenThrow(new RuntimeException("HTTP 500"));
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(scrapeRequestsCounter).increment();
        verify(scrapeFailureCounter).increment();
        verify(scrapeSuccessCounter, never()).increment();
    }

    @Test
    void testScrapeTargetWritesToBuffer() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("# TYPE test_gauge gauge\ntest_gauge 5.0\n");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(buffer).writeAll(any(), eq(BUFFER_WRITE_TIMEOUT_MS));
    }

    @Test
    void testScrapeTargetIncrementsRecordsCreated() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("# TYPE test_gauge gauge\ntest_gauge 5.0\n");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(recordsCreatedCounter).increment(1);
    }

    @Test
    void testScrapeTargetBufferWriteFailureIncrementsFailure() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("# TYPE test_gauge gauge\ntest_gauge 5.0\n");
        setField(service, "scraper", mockScraper);

        doThrow(new RuntimeException("Buffer full")).when(buffer).writeAll(any(), eq(BUFFER_WRITE_TIMEOUT_MS));

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(scrapeFailureCounter).increment();
    }

    @Test
    void testScrapeAllWithEmptyTargets() throws Exception {
        final PrometheusScrapeService service = createService();

        when(config.getTargets()).thenReturn(Collections.emptyList());

        service.scrapeAll();

        verify(scrapeRequestsCounter, never()).increment();
    }

    @Test
    void testStopShutsDownExecutor() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        when(mockExecutor.awaitTermination(11000, TimeUnit.MILLISECONDS)).thenReturn(true);
        setField(service, "executor", mockExecutor);

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        setField(service, "scraper", mockScraper);

        service.stop();

        verify(mockExecutor).shutdown();
        verify(mockExecutor).awaitTermination(11000, TimeUnit.MILLISECONDS);
        verify(mockScraper).close();
    }

    @Test
    void testStopForcesShutdownWhenNotTerminated() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        when(mockExecutor.awaitTermination(11000, TimeUnit.MILLISECONDS)).thenReturn(false);
        setField(service, "executor", mockExecutor);

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        setField(service, "scraper", mockScraper);

        service.stop();

        verify(mockExecutor).shutdown();
        verify(mockExecutor).shutdownNow();
        verify(mockScraper).close();
    }

    @Test
    void testStopHandlesInterruptedException() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
        when(mockExecutor.awaitTermination(11000, TimeUnit.MILLISECONDS))
                .thenThrow(new InterruptedException("interrupted"));
        setField(service, "executor", mockExecutor);

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        setField(service, "scraper", mockScraper);

        service.stop();

        verify(mockExecutor).shutdown();
        verify(mockExecutor).shutdownNow();
        verify(mockScraper).close();
    }

    @Test
    void testScrapeDurationTimerIsUsed() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetScraper mockScraper = mock(ScrapeTargetScraper.class);
        when(mockScraper.scrape(anyString())).thenReturn("");
        setField(service, "scraper", mockScraper);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeTarget("http://localhost:9090/metrics");

        verify(scrapeDurationTimer).record(any(Runnable.class));
    }

    @Test
    void testScrapeAllCatchesExceptionFromScrapeTarget() throws Exception {
        final PrometheusScrapeService service = createService();

        final ScrapeTargetConfig target1 = mock(ScrapeTargetConfig.class);
        when(target1.getUrl()).thenReturn("http://host1:9090/metrics");
        final ScrapeTargetConfig target2 = mock(ScrapeTargetConfig.class);
        when(target2.getUrl()).thenReturn("http://host2:9090/metrics");
        when(config.getTargets()).thenReturn(Arrays.asList(target1, target2));

        doThrow(new RuntimeException("timer failure")).when(scrapeDurationTimer).record(any(Runnable.class));

        service.scrapeAll();

        verify(scrapeRequestsCounter, never()).increment();
    }

    private static void setField(final Object target, final String fieldName, final Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
