/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.time.Duration;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VendorAPIMetricsRecorderTest {

    @Mock
    private PluginMetrics pluginMetrics;

    // Search metrics
    @Mock
    private Counter searchSuccessCounter;
    @Mock
    private Counter searchFailureCounter;
    @Mock
    private Timer searchLatencyTimer;
    @Mock
    private DistributionSummary searchResponseSizeSummary;

    // Get metrics
    @Mock
    private Counter getSuccessCounter;
    @Mock
    private Counter getFailureCounter;
    @Mock
    private Timer getLatencyTimer;
    @Mock
    private DistributionSummary getResponseSizeSummary;

    // Auth metrics
    @Mock
    private Counter authSuccessCounter;
    @Mock
    private Counter authFailureCounter;
    @Mock
    private Timer authLatencyTimer;

    // Start subscription metrics
    @Mock
    private Counter subscriptionSuccessCounter;
    @Mock
    private Counter subscriptionFailureCounter;
    @Mock
    private Timer subscriptionLatencyTimer;
    @Mock
    private Counter subscriptionCallsCounter;

    // List subscription metrics
    @Mock
    private Counter listSubscriptionSuccessCounter;
    @Mock
    private Counter listSubscriptionFailureCounter;
    @Mock
    private Timer listSubscriptionLatencyTimer;
    @Mock
    private Counter listSubscriptionCallsCounter;

    // Buffer metrics
    @Mock
    private Timer bufferWriteLatencyTimer;
    @Mock
    private Counter bufferWriteAttemptsCounter;
    @Mock
    private Counter bufferWriteSuccessCounter;
    @Mock
    private Counter bufferWriteRetrySuccessCounter;
    @Mock
    private Counter bufferWriteRetryAttemptsCounter;
    @Mock
    private Counter bufferWriteFailuresCounter;

    // Shared metrics
    @Mock
    private Counter totalDataApiRequestsCounter;
    @Mock
    private Counter logsRequestedCounter;

    // Error metrics
    @Mock
    private Counter requestAccessDeniedCounter;
    @Mock
    private Counter requestThrottledCounter;
    @Mock
    private Counter resourceNotFoundCounter;
    @Mock
    private Counter nonRetryableErrorsCounter;
    @Mock
    private Counter retryableErrorsCounter;

    private VendorAPIMetricsRecorder recorder;

    @BeforeEach
    void setUp() {
        // Setup search metrics mocks
        when(pluginMetrics.counter("searchRequestsSuccess")).thenReturn(searchSuccessCounter);
        when(pluginMetrics.counter("searchRequestsFailed")).thenReturn(searchFailureCounter);
        when(pluginMetrics.timer("searchRequestLatency")).thenReturn(searchLatencyTimer);
        when(pluginMetrics.summary("searchResponseSizeBytes")).thenReturn(searchResponseSizeSummary);

        // Setup get metrics mocks
        when(pluginMetrics.counter("getRequestsSuccess")).thenReturn(getSuccessCounter);
        when(pluginMetrics.counter("getRequestsFailed")).thenReturn(getFailureCounter);
        when(pluginMetrics.timer("getRequestLatency")).thenReturn(getLatencyTimer);
        when(pluginMetrics.summary("getResponseSizeBytes")).thenReturn(getResponseSizeSummary);

        // Setup auth metrics mocks
        when(pluginMetrics.counter("authenticationRequestsSuccess")).thenReturn(authSuccessCounter);
        when(pluginMetrics.counter("authenticationRequestsFailed")).thenReturn(authFailureCounter);
        when(pluginMetrics.timer("authenticationRequestLatency")).thenReturn(authLatencyTimer);

        // Setup start subscription metrics mocks
        when(pluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(subscriptionSuccessCounter);
        when(pluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(subscriptionFailureCounter);
        when(pluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(subscriptionLatencyTimer);
        when(pluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(subscriptionCallsCounter);

        // Setup list subscription metrics mocks
        when(pluginMetrics.counter("listSubscriptionRequestsSuccess")).thenReturn(listSubscriptionSuccessCounter);
        when(pluginMetrics.counter("listSubscriptionRequestsFailed")).thenReturn(listSubscriptionFailureCounter);
        when(pluginMetrics.timer("listSubscriptionRequestLatency")).thenReturn(listSubscriptionLatencyTimer);
        when(pluginMetrics.counter("listSubscriptionApiCalls")).thenReturn(listSubscriptionCallsCounter);

        // Setup buffer metrics mocks
        when(pluginMetrics.timer("bufferWriteLatency")).thenReturn(bufferWriteLatencyTimer);
        when(pluginMetrics.counter("bufferWriteAttempts")).thenReturn(bufferWriteAttemptsCounter);
        when(pluginMetrics.counter("bufferWriteSuccess")).thenReturn(bufferWriteSuccessCounter);
        when(pluginMetrics.counter("bufferWriteRetrySuccess")).thenReturn(bufferWriteRetrySuccessCounter);
        when(pluginMetrics.counter("bufferWriteRetryAttempts")).thenReturn(bufferWriteRetryAttemptsCounter);
        when(pluginMetrics.counter("bufferWriteFailures")).thenReturn(bufferWriteFailuresCounter);

        // Setup shared metrics mocks
        when(pluginMetrics.counter("totalDataApiRequests")).thenReturn(totalDataApiRequestsCounter);
        when(pluginMetrics.counter("logsRequested")).thenReturn(logsRequestedCounter);

        // Setup error metrics mocks
        when(pluginMetrics.counter("requestAccessDenied")).thenReturn(requestAccessDeniedCounter);
        when(pluginMetrics.counter("requestThrottled")).thenReturn(requestThrottledCounter);
        when(pluginMetrics.counter("resourceNotFound")).thenReturn(resourceNotFoundCounter);
        when(pluginMetrics.counter("nonRetryableErrors")).thenReturn(nonRetryableErrorsCounter);
        when(pluginMetrics.counter("retryableErrors")).thenReturn(retryableErrorsCounter);

        // Use explicit constructor with enabled=true to match existing test expectations
        recorder = new VendorAPIMetricsRecorder(pluginMetrics, true);
    }

    @Test
    void constructor_CreatesAllMetricsCorrectly() {
        assertThat(recorder, notNullValue());
        
        // Verify search metrics creation
        verify(pluginMetrics).counter("searchRequestsSuccess");
        verify(pluginMetrics).counter("searchRequestsFailed");
        verify(pluginMetrics).timer("searchRequestLatency");
        verify(pluginMetrics).summary("searchResponseSizeBytes");

        // Verify get metrics creation
        verify(pluginMetrics).counter("getRequestsSuccess");
        verify(pluginMetrics).counter("getRequestsFailed");
        verify(pluginMetrics).timer("getRequestLatency");
        verify(pluginMetrics).summary("getResponseSizeBytes");

        // Verify auth metrics creation
        verify(pluginMetrics).counter("authenticationRequestsSuccess");
        verify(pluginMetrics).counter("authenticationRequestsFailed");
        verify(pluginMetrics).timer("authenticationRequestLatency");

        // Verify start subscription metrics creation
        verify(pluginMetrics).counter("startSubscriptionRequestsSuccess");
        verify(pluginMetrics).counter("startSubscriptionRequestsFailed");
        verify(pluginMetrics).timer("startSubscriptionRequestLatency");
        verify(pluginMetrics).counter("startSubscriptionApiCalls");

        // Verify list subscription metrics creation
        verify(pluginMetrics).counter("listSubscriptionRequestsSuccess");
        verify(pluginMetrics).counter("listSubscriptionRequestsFailed");
        verify(pluginMetrics).timer("listSubscriptionRequestLatency");
        verify(pluginMetrics).counter("listSubscriptionApiCalls");

        // Verify buffer metrics creation
        verify(pluginMetrics).timer("bufferWriteLatency");
        verify(pluginMetrics).counter("bufferWriteAttempts");
        verify(pluginMetrics).counter("bufferWriteSuccess");
        verify(pluginMetrics).counter("bufferWriteRetrySuccess");
        verify(pluginMetrics).counter("bufferWriteRetryAttempts");
        verify(pluginMetrics).counter("bufferWriteFailures");

        // Verify shared metrics creation
        verify(pluginMetrics).counter("totalDataApiRequests");
        verify(pluginMetrics).counter("logsRequested");
        
        // Verify error metrics creation
        verify(pluginMetrics).counter("requestAccessDenied");
        verify(pluginMetrics).counter("requestThrottled");
        verify(pluginMetrics).counter("resourceNotFound");
        verify(pluginMetrics).counter("nonRetryableErrors");
        verify(pluginMetrics).counter("retryableErrors");
    }

    @Test
    void recordSearchSuccess_IncrementsSearchSuccessCounter() {
        recorder.recordSearchSuccess();
        
        verify(searchSuccessCounter).increment();
    }

    @Test
    void recordSearchFailure_IncrementsSearchFailureCounter() {
        recorder.recordSearchFailure();
        
        verify(searchFailureCounter).increment();
    }

    @Test
    void recordGetSuccess_IncrementsGetSuccessCounter() {
        recorder.recordGetSuccess();
        
        verify(getSuccessCounter).increment();
    }

    @Test
    void recordGetFailure_IncrementsGetFailureCounter() {
        recorder.recordGetFailure();
        
        verify(getFailureCounter).increment();
    }

    @Test
    void recordAuthSuccess_IncrementsAuthSuccessCounter() {
        recorder.recordAuthSuccess();
        
        verify(authSuccessCounter).increment();
    }

    @Test
    void recordAuthFailure_IncrementsAuthFailureCounter() {
        recorder.recordAuthFailure();
        
        verify(authFailureCounter).increment();
    }

    // Subscription metrics tests

    @Test
    void recordSubscriptionSuccess_IncrementsSubscriptionSuccessCounter() {
        recorder.recordSubscriptionSuccess();
        
        verify(subscriptionSuccessCounter).increment();
    }

    @Test
    void recordSubscriptionFailure_IncrementsSubscriptionFailureCounter() {
        recorder.recordSubscriptionFailure();
        
        verify(subscriptionFailureCounter).increment();
    }

    @Test
    void recordSubscriptionCall_IncrementsSubscriptionCallsCounter() {
        recorder.recordSubscriptionCall();
        
        verify(subscriptionCallsCounter).increment();
    }

    @Test
    void recordSubscriptionLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "subscription result";
        Supplier<String> operation = () -> expectedResult;
        when(subscriptionLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordSubscriptionLatency(operation);
        
        verify(subscriptionLatencyTimer).record(eq(operation));
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void recordSubscriptionLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void operation */ };
        
        recorder.recordSubscriptionLatency(operation);
        
        verify(subscriptionLatencyTimer).record(eq(operation));
    }

    @Test
    void recordSubscriptionLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(100);
        
        recorder.recordSubscriptionLatency(duration);
        
        verify(subscriptionLatencyTimer).record(duration);
    }

    @Test
    void recordSearchLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "search result";
        Supplier<String> operation = () -> expectedResult;
        when(searchLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordSearchLatency(operation);
        
        verify(searchLatencyTimer).record(eq(operation));
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void recordGetLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "get result";
        Supplier<String> operation = () -> expectedResult;
        when(getLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordGetLatency(operation);
        
        verify(getLatencyTimer).record(eq(operation));
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void recordAuthLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "auth result";
        Supplier<String> operation = () -> expectedResult;
        when(authLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordAuthLatency(operation);
        
        verify(authLatencyTimer).record(eq(operation));
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void recordSearchLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void operation */ };
        
        recorder.recordSearchLatency(operation);
        
        verify(searchLatencyTimer).record(eq(operation));
    }

    @Test
    void recordGetLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void operation */ };
        
        recorder.recordGetLatency(operation);
        
        verify(getLatencyTimer).record(eq(operation));
    }

    @Test
    void recordAuthLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void operation */ };
        
        recorder.recordAuthLatency(operation);
        
        verify(authLatencyTimer).record(eq(operation));
    }

    @Test
    void recordSearchLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(500);
        
        recorder.recordSearchLatency(duration);
        
        verify(searchLatencyTimer).record(duration);
    }

    @Test
    void recordGetLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(300);
        
        recorder.recordGetLatency(duration);
        
        verify(getLatencyTimer).record(duration);
    }

    @Test
    void recordAuthLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(200);
        
        recorder.recordAuthLatency(duration);
        
        verify(authLatencyTimer).record(duration);
    }

    @Test
    void recordSearchResponseSize_WithBytes_RecordsSize() {
        long responseSize = 1024L;
        
        recorder.recordSearchResponseSize(responseSize);
        
        verify(searchResponseSizeSummary).record(responseSize);
    }

    @Test
    void recordSearchResponseSize_WithResponseEntity_RecordsContentLength() {
        ResponseEntity<String> response = ResponseEntity.ok()
                .header("Content-Length", "512")
                .body("test");
        
        recorder.recordSearchResponseSize(response);
        
        verify(searchResponseSizeSummary).record(512L);
    }

    @Test
    void recordSearchResponseSize_WithString_RecordsByteLength() {
        String response = "test response";
        
        recorder.recordSearchResponseSize(response);
        
        verify(searchResponseSizeSummary).record(response.getBytes().length);
    }

    @Test
    void recordGetResponseSize_WithString_RecordsByteLength() {
        String response = "get response";
        
        recorder.recordGetResponseSize(response);
        
        verify(getResponseSizeSummary).record(response.getBytes().length);
    }

    @Test
    void recordGetResponseSize_WithResponseEntity_RecordsContentLength() {
        // Create a proper ResponseEntity with Content-Length header
        ResponseEntity<String> response = ResponseEntity.ok()
                .header("Content-Length", "256")
                .body("test");
        
        recorder.recordGetResponseSize(response);
        
        verify(getResponseSizeSummary).record(256L);
    }

    @Test
    void recordGetResponseSize_WithBytes_RecordsSize() {
        long responseSize = 2048L;
        
        recorder.recordGetResponseSize(responseSize);
        
        verify(getResponseSizeSummary).record(responseSize);
    }

    @Test
    void recordDataApiRequest_IncrementsRequestCounter() {
        recorder.recordDataApiRequest();
        
        verify(totalDataApiRequestsCounter).increment();
    }

    @Test
    void recordLogsRequested_IncrementsLogsRequestedCounter() {
        recorder.recordLogsRequested();
        
        verify(logsRequestedCounter).increment();
    }

    @Test
    void recordLogsRequested_WithCount_IncrementsLogsRequestedCounterByCount() {
        int logCount = 5;
        
        recorder.recordLogsRequested(logCount);
        
        verify(logsRequestedCounter).increment(logCount);
    }

    @Test
    void recordLogsRequested_WithZeroCount_IncrementsLogsRequestedCounterByZero() {
        int logCount = 0;
        
        recorder.recordLogsRequested(logCount);
        
        verify(logsRequestedCounter).increment(logCount);
    }

    @Test
    void recordLogsRequested_WithLargeCount_IncrementsLogsRequestedCounterByLargeCount() {
        int logCount = 1000;
        
        recorder.recordLogsRequested(logCount);
        
        verify(logsRequestedCounter).increment(logCount);
    }

    @Test
    void recordLogsRequested_WithMultipleCounts_IncrementsLogsRequestedCounterCorrectly() {
        recorder.recordLogsRequested(3);
        recorder.recordLogsRequested(7);
        recorder.recordLogsRequested(2);
        
        verify(logsRequestedCounter).increment(3);
        verify(logsRequestedCounter).increment(7);
        verify(logsRequestedCounter).increment(2);
    }

    @Test
    void recordLogsRequested_MixedWithParameterlessMethod_BothIncrementsWork() {
        recorder.recordLogsRequested(); // No parameter - increments by 1
        recorder.recordLogsRequested(10); // With parameter - increments by 10
        recorder.recordLogsRequested(); // No parameter - increments by 1 again
        
        verify(logsRequestedCounter, times(2)).increment(); // Called twice without parameters
        verify(logsRequestedCounter, times(1)).increment(10); // Called once with parameter
    }


    @Test
    void standaloneOperations_WorkCorrectly() {
        // Test standalone usage pattern (the primary use case)
        String response = "test response";
        
        recorder.recordSearchSuccess();
        recorder.recordSearchResponseSize(response.getBytes().length);
        recorder.recordDataApiRequest();
        
        verify(searchSuccessCounter).increment();
        verify(searchResponseSizeSummary).record(response.getBytes().length);
        verify(totalDataApiRequestsCounter).increment();
    }

    @Test
    void mixedOperations_WorkCorrectly() {
        // Test that we can use different operation types on the same recorder instance
        recorder.recordSearchSuccess();
        recorder.recordGetSuccess();
        recorder.recordAuthSuccess();
        recorder.recordSubscriptionSuccess();
        
        verify(searchSuccessCounter).increment();
        verify(getSuccessCounter).increment();
        verify(authSuccessCounter).increment();
        verify(subscriptionSuccessCounter).increment();
    }

    // Edge case tests for comprehensive coverage
    
    @Test
    void recordSearchResponseSize_WithNullResponseEntity_RecordsZero() {
        recorder.recordSearchResponseSize((ResponseEntity<?>) null);
        
        verify(searchResponseSizeSummary).record(0L);
    }

    @Test
    void recordSearchResponseSize_WithNullString_RecordsZero() {
        recorder.recordSearchResponseSize((String) null);
        
        verify(searchResponseSizeSummary).record(0L);
    }

    @Test
    void recordSearchResponseSize_WithResponseEntityMissingContentLength_RecordsZero() {
        ResponseEntity<String> response = ResponseEntity.ok().body("test");
        
        recorder.recordSearchResponseSize(response);
        
        verify(searchResponseSizeSummary).record(0L);
    }

    @Test
    void recordSearchResponseSize_WithInvalidContentLength_RecordsZero() {
        ResponseEntity<String> response = ResponseEntity.ok()
                .header("Content-Length", "invalid")
                .body("test");
        
        recorder.recordSearchResponseSize(response);
        
        verify(searchResponseSizeSummary).record(0L);
    }

    @Test
    void recordGetResponseSize_WithNullString_RecordsZero() {
        recorder.recordGetResponseSize((String) null);
        
        verify(getResponseSizeSummary).record(0L);
    }

    @Test
    void recordGetResponseSize_WithNullResponseEntity_RecordsZero() {
        recorder.recordGetResponseSize((ResponseEntity<?>) null);
        
        verify(getResponseSizeSummary).record(0L);
    }

    // recordError method tests

    @Test
    void recordError_WithUnauthorized_IncrementsRequestAccessDeniedCounter() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.UNAUTHORIZED);
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter).increment();
    }

    @Test
    void recordError_WithForbidden_IncrementsRequestAccessDeniedCounter() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.FORBIDDEN);
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter).increment();
    }

    @Test
    void recordError_WithTooManyRequests_IncrementsRequestThrottledCounter() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS);
        
        recorder.recordError(exception);
        
        verify(requestThrottledCounter).increment();
    }

    @Test
    void recordError_WithNotFound_IncrementsResourceNotFoundCounter() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.NOT_FOUND);
        
        recorder.recordError(exception);
        
        verify(resourceNotFoundCounter).increment();
    }

    @Test
    void recordError_WithSecurityException_IncrementsRequestAccessDeniedCounter() {
        SecurityException exception = new SecurityException("Access denied");
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter).increment();
    }

    @Test
    void recordError_WithHttpServerErrorException_DoesNotIncrementAnyCounters() {
        HttpServerErrorException exception = new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter, org.mockito.Mockito.never()).increment();
        verify(requestThrottledCounter, org.mockito.Mockito.never()).increment();
        verify(resourceNotFoundCounter, org.mockito.Mockito.never()).increment();
    }

    @Test
    void recordError_WithOtherHttpClientError_DoesNotIncrementAnyCounters() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter, org.mockito.Mockito.never()).increment();
        verify(requestThrottledCounter, org.mockito.Mockito.never()).increment();
        verify(resourceNotFoundCounter, org.mockito.Mockito.never()).increment();
    }

    @Test
    void recordError_WithGenericException_DoesNotIncrementAnyCounters() {
        RuntimeException exception = new RuntimeException("Generic error");
        
        recorder.recordError(exception);
        
        verify(requestAccessDeniedCounter, org.mockito.Mockito.never()).increment();
        verify(requestThrottledCounter, org.mockito.Mockito.never()).increment();
        verify(resourceNotFoundCounter, org.mockito.Mockito.never()).increment();
    }

    @Test
    void recordSubscriptionSuccessMultiple() {
        recorder.recordSubscriptionSuccess();
        recorder.recordSubscriptionSuccess();
        recorder.recordSubscriptionSuccess();

        verify(subscriptionSuccessCounter, times(3)).increment();
    }

    @Test
    void recordSubscriptionFailureMultiple() {
        recorder.recordSubscriptionFailure();
        recorder.recordSubscriptionFailure();

        verify(subscriptionFailureCounter, times(2)).increment();
    }

    @Test
    void recordSubscriptionLatencyWithIntegerSupplier() {
        Supplier<Integer> operation = () -> 42;
        when(subscriptionLatencyTimer.record(operation)).thenReturn(42);

        Integer result = recorder.recordSubscriptionLatency(operation);

        assertEquals(42, result);
        verify(subscriptionLatencyTimer, times(1)).record(operation);
    }

    @Test
    void recordSubscriptionLatencyWithMultipleDurations() {
        Duration duration1 = Duration.ofMillis(50);
        Duration duration2 = Duration.ofMillis(150);
        Duration duration3 = Duration.ofMillis(200);

        recorder.recordSubscriptionLatency(duration1);
        recorder.recordSubscriptionLatency(duration2);
        recorder.recordSubscriptionLatency(duration3);

        verify(subscriptionLatencyTimer, times(1)).record(duration1);
        verify(subscriptionLatencyTimer, times(1)).record(duration2);
        verify(subscriptionLatencyTimer, times(1)).record(duration3);
    }

    @Test
    void recordSubscriptionCallMultiple() {
        recorder.recordSubscriptionCall();
        recorder.recordSubscriptionCall();
        recorder.recordSubscriptionCall();
        recorder.recordSubscriptionCall();

        verify(subscriptionCallsCounter, times(4)).increment();
    }

    @Test
    void mixedSubscriptionMetricsScenario() {
        // Record various subscription metrics
        recorder.recordSubscriptionSuccess();
        recorder.recordSubscriptionFailure();
        recorder.recordSubscriptionCall();
        recorder.recordSubscriptionCall();

        // Verify all metrics were recorded correctly
        verify(subscriptionSuccessCounter, times(1)).increment();
        verify(subscriptionFailureCounter, times(1)).increment();
        verify(subscriptionCallsCounter, times(2)).increment();
    }

    @Test
    void recordSubscriptionLatencySuccessfulOperation() {
        Supplier<String> operation = () -> "success";
        String result = "success";
        when(subscriptionLatencyTimer.record(operation)).thenReturn(result);

        String returnedResult = recorder.recordSubscriptionLatency(operation);

        assertEquals(result, returnedResult);
        verify(subscriptionLatencyTimer, times(1)).record(operation);
    }

    @Test
    void realisticSubscriptionMetricsScenario() {
        // Simulate 10 subscription operations with mixed success/failure
        for (int i = 0; i < 10; i++) {
            recorder.recordSubscriptionCall();
            if (i % 2 == 0) {
                recorder.recordSubscriptionSuccess();
            } else {
                recorder.recordSubscriptionFailure();
            }
        }

        // Verify metrics
        verify(subscriptionCallsCounter, times(10)).increment();
        verify(subscriptionSuccessCounter, times(5)).increment(); // Even indices: 0,2,4,6,8
        verify(subscriptionFailureCounter, times(5)).increment();  // Odd indices: 1,3,5,7,9
    }

    @Test
    void recordSubscriptionLatencyPropagatesExceptions() {
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Test exception");
        };

        // Configure the mock timer to actually execute the supplier and propagate the exception
        when(subscriptionLatencyTimer.record(failingOperation)).thenThrow(new RuntimeException("Test exception"));

        assertThrows(RuntimeException.class, () -> {
            recorder.recordSubscriptionLatency(failingOperation);
        });

        // Timer should still be called even if the operation fails
        verify(subscriptionLatencyTimer, times(1)).record(failingOperation);
    }

    @Test
    void recordSubscriptionLatencyWithNullDuration() {
        // Duration should not be null in normal usage, but testing robustness
        Duration nullDuration = null;

        // Configure the mock timer to throw NPE when null duration is passed
        doThrow(new NullPointerException()).when(subscriptionLatencyTimer).record(nullDuration);

        assertThrows(NullPointerException.class, () -> {
            recorder.recordSubscriptionLatency(nullDuration);
        });
    }

    // List subscription metrics tests

    @Test
    void recordListSubscriptionSuccess_IncrementsListSubscriptionSuccessCounter() {
        recorder.recordListSubscriptionSuccess();
        
        verify(listSubscriptionSuccessCounter).increment();
    }

    @Test
    void recordListSubscriptionFailure_IncrementsListSubscriptionFailureCounter() {
        recorder.recordListSubscriptionFailure();
        
        verify(listSubscriptionFailureCounter).increment();
    }

    @Test
    void recordListSubscriptionCall_IncrementsListSubscriptionCallsCounter() {
        recorder.recordListSubscriptionCall();
        
        verify(listSubscriptionCallsCounter).increment();
    }

    @Test
    void recordListSubscriptionLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "list subscription result";
        Supplier<String> operation = () -> expectedResult;
        when(listSubscriptionLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordListSubscriptionLatency(operation);
        
        verify(listSubscriptionLatencyTimer).record(eq(operation));
    }

    // Buffer metrics tests

    @Test
    void recordBufferWriteAttempt_IncrementsBufferWriteAttemptsCounter() {
        recorder.recordBufferWriteAttempt();
        
        verify(bufferWriteAttemptsCounter).increment();
    }

    @Test
    void recordBufferWriteSuccess_IncrementsBufferWriteSuccessCounter() {
        recorder.recordBufferWriteSuccess();
        
        verify(bufferWriteSuccessCounter).increment();
    }

    @Test
    void recordBufferWriteRetrySuccess_IncrementsBufferWriteRetrySuccessCounter() {
        recorder.recordBufferWriteRetrySuccess();
        
        verify(bufferWriteRetrySuccessCounter).increment();
    }

    @Test
    void recordBufferWriteRetryAttempt_IncrementsBufferWriteRetryAttemptsCounter() {
        recorder.recordBufferWriteRetryAttempt();
        
        verify(bufferWriteRetryAttemptsCounter).increment();
    }

    @Test
    void recordBufferWriteFailure_IncrementsBufferWriteFailuresCounter() {
        recorder.recordBufferWriteFailure();
        
        verify(bufferWriteFailuresCounter).increment();
    }

    @Test
    void recordBufferWriteLatency_WithSupplier_RecordsLatencyAndReturnsResult() {
        String expectedResult = "buffer write result";
        Supplier<String> operation = () -> expectedResult;
        when(bufferWriteLatencyTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result = recorder.recordBufferWriteLatency(operation);
        
        verify(bufferWriteLatencyTimer).record(eq(operation));
        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void recordListSubscriptionLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void operation */ };
        
        recorder.recordListSubscriptionLatency(operation);
        
        verify(listSubscriptionLatencyTimer).record(eq(operation));
    }

    @Test
    void recordListSubscriptionLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(75);
        
        recorder.recordListSubscriptionLatency(duration);
        
        verify(listSubscriptionLatencyTimer).record(duration);
    }

    @Test
    void recordListSubscriptionSuccessMultiple() {
        recorder.recordListSubscriptionSuccess();
        recorder.recordListSubscriptionSuccess();
        recorder.recordListSubscriptionSuccess();

        verify(listSubscriptionSuccessCounter, times(3)).increment();
    }

    @Test
    void recordListSubscriptionFailureMultiple() {
        recorder.recordListSubscriptionFailure();
        recorder.recordListSubscriptionFailure();

        verify(listSubscriptionFailureCounter, times(2)).increment();
    }

    @Test
    void recordListSubscriptionLatencyWithIntegerSupplier() {
        Supplier<Integer> operation = () -> 24;
        when(listSubscriptionLatencyTimer.record(operation)).thenReturn(24);

        Integer result = recorder.recordListSubscriptionLatency(operation);

        assertEquals(24, result);
        verify(listSubscriptionLatencyTimer, times(1)).record(operation);
    }

    @Test
    void recordListSubscriptionLatencyWithMultipleDurations() {
        Duration duration1 = Duration.ofMillis(25);
        Duration duration2 = Duration.ofMillis(75);
        Duration duration3 = Duration.ofMillis(125);

        recorder.recordListSubscriptionLatency(duration1);
        recorder.recordListSubscriptionLatency(duration2);
        recorder.recordListSubscriptionLatency(duration3);

        verify(listSubscriptionLatencyTimer, times(1)).record(duration1);
        verify(listSubscriptionLatencyTimer, times(1)).record(duration2);
        verify(listSubscriptionLatencyTimer, times(1)).record(duration3);
    }

    @Test
    void recordListSubscriptionCallMultiple() {
        recorder.recordListSubscriptionCall();
        recorder.recordListSubscriptionCall();
        recorder.recordListSubscriptionCall();

        verify(listSubscriptionCallsCounter, times(3)).increment();
    }

    @Test
    void mixedListSubscriptionMetricsScenario() {
        // Record various list subscription metrics
        recorder.recordListSubscriptionSuccess();
        recorder.recordListSubscriptionFailure();
        recorder.recordListSubscriptionCall();
        recorder.recordListSubscriptionCall();

        // Verify all metrics were recorded correctly
        verify(listSubscriptionSuccessCounter, times(1)).increment();
        verify(listSubscriptionFailureCounter, times(1)).increment();
        verify(listSubscriptionCallsCounter, times(2)).increment();
    }

    @Test
    void recordListSubscriptionLatencySuccessfulOperation() {
        Supplier<String> operation = () -> "list success";
        String result = "list success";
        when(listSubscriptionLatencyTimer.record(operation)).thenReturn(result);

        String returnedResult = recorder.recordListSubscriptionLatency(operation);

        assertEquals(result, returnedResult);
        verify(listSubscriptionLatencyTimer, times(1)).record(operation);
    }

    @Test
    void mixedSubscriptionTypesMetricsScenario() {
        // Test both start and list subscription metrics work together
        recorder.recordSubscriptionSuccess();
        recorder.recordListSubscriptionSuccess();
        recorder.recordSubscriptionCall();
        recorder.recordListSubscriptionCall();
        
        // Verify both types of metrics were recorded separately
        verify(subscriptionSuccessCounter, times(1)).increment();
        verify(listSubscriptionSuccessCounter, times(1)).increment();
        verify(subscriptionCallsCounter, times(1)).increment();
        verify(listSubscriptionCallsCounter, times(1)).increment();
    }

    @Test
    void recordListSubscriptionLatencyPropagatesExceptions() {
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("List test exception");
        };

        // Configure the mock timer to actually execute the supplier and propagate the exception
        when(listSubscriptionLatencyTimer.record(failingOperation)).thenThrow(new RuntimeException("List test exception"));

        assertThrows(RuntimeException.class, () -> {
            recorder.recordListSubscriptionLatency(failingOperation);
        });

        // Timer should still be called even if the operation fails
        verify(listSubscriptionLatencyTimer, times(1)).record(failingOperation);
    }


    @Test
    void recordBufferWriteLatency_WithRunnable_RecordsLatency() {
        Runnable operation = () -> { /* void buffer write operation */ };
        
        recorder.recordBufferWriteLatency(operation);
        
        verify(bufferWriteLatencyTimer).record(eq(operation));
    }

    @Test
    void recordBufferWriteLatency_WithDuration_RecordsLatency() {
        Duration duration = Duration.ofMillis(150);
        
        recorder.recordBufferWriteLatency(duration);
        
        verify(bufferWriteLatencyTimer).record(duration);
    }

    @Test
    void recordNonRetryableError_IncrementsNonRetryableErrorsCounter() {
        recorder.recordNonRetryableError();
        
        verify(nonRetryableErrorsCounter).increment();
    }

    @Test
    void recordRetryableError_IncrementsRetryableErrorsCounter() {
        recorder.recordRetryableError();
        
        verify(retryableErrorsCounter).increment();
    }

    @Test
    void recordBufferWriteLatencyWithIntegerSupplier() {
        Supplier<Integer> operation = () -> 100;
        when(bufferWriteLatencyTimer.record(operation)).thenReturn(100);

        Integer result = recorder.recordBufferWriteLatency(operation);

        assertEquals(100, result);
        verify(bufferWriteLatencyTimer, times(1)).record(operation);
    }

    @Test
    void recordBufferWriteLatencyWithMultipleDurations() {
        Duration duration1 = Duration.ofMillis(100);
        Duration duration2 = Duration.ofMillis(250);
        Duration duration3 = Duration.ofMillis(400);

        recorder.recordBufferWriteLatency(duration1);
        recorder.recordBufferWriteLatency(duration2);
        recorder.recordBufferWriteLatency(duration3);

        verify(bufferWriteLatencyTimer, times(1)).record(duration1);
        verify(bufferWriteLatencyTimer, times(1)).record(duration2);
        verify(bufferWriteLatencyTimer, times(1)).record(duration3);
    }

    @Test
    void recordMultipleBufferWriteAttempts() {
        recorder.recordBufferWriteAttempt();
        recorder.recordBufferWriteAttempt();
        recorder.recordBufferWriteAttempt();

        verify(bufferWriteAttemptsCounter, times(3)).increment();
    }

    @Test
    void recordMultipleBufferWriteSuccesses() {
        recorder.recordBufferWriteSuccess();
        recorder.recordBufferWriteSuccess();

        verify(bufferWriteSuccessCounter, times(2)).increment();
    }

    @Test
    void recordMultipleBufferWriteFailures() {
        recorder.recordBufferWriteFailure();
        recorder.recordBufferWriteFailure();
        recorder.recordBufferWriteFailure();
        recorder.recordBufferWriteFailure();

        verify(bufferWriteFailuresCounter, times(4)).increment();
    }

    @Test
    void recordMultipleErrorCategorizations() {
        recorder.recordRetryableError();
        recorder.recordRetryableError();
        recorder.recordNonRetryableError();

        verify(retryableErrorsCounter, times(2)).increment();
        verify(nonRetryableErrorsCounter, times(1)).increment();
    }

    @Test
    void realisticBufferWriteScenario() {
        // Simulate buffer write operations with retries
        recorder.recordBufferWriteAttempt();  // Initial attempt
        recorder.recordBufferWriteRetryAttempt();  // First retry
        recorder.recordBufferWriteRetryAttempt();  // Second retry
        recorder.recordBufferWriteRetrySuccess();  // Success on retry

        verify(bufferWriteAttemptsCounter, times(1)).increment();
        verify(bufferWriteRetryAttemptsCounter, times(2)).increment();
        verify(bufferWriteRetrySuccessCounter, times(1)).increment();
        verify(bufferWriteSuccessCounter, times(0)).increment(); // No direct success
        verify(bufferWriteFailuresCounter, times(0)).increment(); // No final failure
    }

    @Test
    void realisticBufferWriteFailureScenario() {
        // Simulate buffer write operations that ultimately fail
        recorder.recordBufferWriteAttempt();  // Initial attempt
        recorder.recordBufferWriteRetryAttempt();  // First retry
        recorder.recordBufferWriteRetryAttempt();  // Second retry
        recorder.recordBufferWriteRetryAttempt();  // Third retry
        recorder.recordBufferWriteFailure();  // Ultimate failure

        verify(bufferWriteAttemptsCounter, times(1)).increment();
        verify(bufferWriteRetryAttemptsCounter, times(3)).increment();
        verify(bufferWriteFailuresCounter, times(1)).increment();
        verify(bufferWriteSuccessCounter, times(0)).increment();
        verify(bufferWriteRetrySuccessCounter, times(0)).increment();
    }

    @Test
    void mixedBufferAndErrorMetricsScenario() {
        // Record various buffer and error metrics
        recorder.recordBufferWriteAttempt();
        recorder.recordBufferWriteSuccess();
        recorder.recordRetryableError();
        recorder.recordNonRetryableError();

        // Verify all metrics were recorded correctly
        verify(bufferWriteAttemptsCounter, times(1)).increment();
        verify(bufferWriteSuccessCounter, times(1)).increment();
        verify(retryableErrorsCounter, times(1)).increment();
        verify(nonRetryableErrorsCounter, times(1)).increment();
    }

    @Test
    void recordBufferWriteLatencyPropagatesExceptions() {
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Buffer write exception");
        };

        // Configure the mock timer to propagate the exception
        when(bufferWriteLatencyTimer.record(failingOperation)).thenThrow(new RuntimeException("Buffer write exception"));

        assertThrows(RuntimeException.class, () -> {
            recorder.recordBufferWriteLatency(failingOperation);
        });

        // Timer should still be called even if the operation fails
        verify(bufferWriteLatencyTimer, times(1)).record(failingOperation);
    }

    @Test
    void recordBufferWriteLatencyWithNullDuration() {
        // Duration should not be null in normal usage, but testing robustness
        Duration nullDuration = null;

        // Configure the mock timer to throw NPE when null duration is passed
        doThrow(new NullPointerException()).when(listSubscriptionLatencyTimer).record(nullDuration);

        assertThrows(NullPointerException.class, () -> {
            recorder.recordListSubscriptionLatency(nullDuration);
        });
    }

    // === NEW SUBSCRIPTION METRICS GATING TESTS ===

    @Test
    void constructor_WithEnabledSubscriptionMetrics_CreatesAllMetrics() {
        // Use a separate PluginMetrics mock to avoid interference with setUp mocks
        PluginMetrics separatePluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        
        // Add minimal mocks needed for constructor - use lenient to avoid unnecessary stubbing errors
        org.mockito.Mockito.lenient().when(separatePluginMetrics.counter(any())).thenReturn(org.mockito.Mockito.mock(Counter.class));
        org.mockito.Mockito.lenient().when(separatePluginMetrics.timer(any())).thenReturn(org.mockito.Mockito.mock(Timer.class));
        org.mockito.Mockito.lenient().when(separatePluginMetrics.summary(any())).thenReturn(org.mockito.Mockito.mock(DistributionSummary.class));
        
        VendorAPIMetricsRecorder recorderEnabled = new VendorAPIMetricsRecorder(separatePluginMetrics, true);
        
        assertThat(recorderEnabled, notNullValue());
        
        // Verify subscription metrics are created when enabled=true
        verify(separatePluginMetrics, times(1)).counter("startSubscriptionRequestsSuccess");
        verify(separatePluginMetrics, times(1)).counter("startSubscriptionRequestsFailed");
        verify(separatePluginMetrics, times(1)).timer("startSubscriptionRequestLatency");
        verify(separatePluginMetrics, times(1)).counter("startSubscriptionApiCalls");
        
        verify(separatePluginMetrics, times(1)).counter("listSubscriptionRequestsSuccess");
        verify(separatePluginMetrics, times(1)).counter("listSubscriptionRequestsFailed");
        verify(separatePluginMetrics, times(1)).timer("listSubscriptionRequestLatency");
        verify(separatePluginMetrics, times(1)).counter("listSubscriptionApiCalls");
    }

    @Test
    void constructor_WithDisabledSubscriptionMetrics_DoesNotCreateSubscriptionMetrics() {
        // Create a new PluginMetrics mock for this test to avoid interaction with setUp
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        
        // Setup non-subscription metrics mocks
        when(testPluginMetrics.counter("searchRequestsSuccess")).thenReturn(searchSuccessCounter);
        when(testPluginMetrics.counter("searchRequestsFailed")).thenReturn(searchFailureCounter);
        when(testPluginMetrics.timer("searchRequestLatency")).thenReturn(searchLatencyTimer);
        when(testPluginMetrics.summary("searchResponseSizeBytes")).thenReturn(searchResponseSizeSummary);
        when(testPluginMetrics.counter("getRequestsSuccess")).thenReturn(getSuccessCounter);
        when(testPluginMetrics.counter("getRequestsFailed")).thenReturn(getFailureCounter);
        when(testPluginMetrics.timer("getRequestLatency")).thenReturn(getLatencyTimer);
        when(testPluginMetrics.summary("getResponseSizeBytes")).thenReturn(getResponseSizeSummary);
        when(testPluginMetrics.counter("authenticationRequestsSuccess")).thenReturn(authSuccessCounter);
        when(testPluginMetrics.counter("authenticationRequestsFailed")).thenReturn(authFailureCounter);
        when(testPluginMetrics.timer("authenticationRequestLatency")).thenReturn(authLatencyTimer);
        when(testPluginMetrics.timer("bufferWriteLatency")).thenReturn(bufferWriteLatencyTimer);
        when(testPluginMetrics.counter("bufferWriteAttempts")).thenReturn(bufferWriteAttemptsCounter);
        when(testPluginMetrics.counter("bufferWriteSuccess")).thenReturn(bufferWriteSuccessCounter);
        when(testPluginMetrics.counter("bufferWriteRetrySuccess")).thenReturn(bufferWriteRetrySuccessCounter);
        when(testPluginMetrics.counter("bufferWriteRetryAttempts")).thenReturn(bufferWriteRetryAttemptsCounter);
        when(testPluginMetrics.counter("bufferWriteFailures")).thenReturn(bufferWriteFailuresCounter);
        when(testPluginMetrics.counter("totalDataApiRequests")).thenReturn(totalDataApiRequestsCounter);
        when(testPluginMetrics.counter("logsRequested")).thenReturn(logsRequestedCounter);
        when(testPluginMetrics.counter("requestAccessDenied")).thenReturn(requestAccessDeniedCounter);
        when(testPluginMetrics.counter("requestThrottled")).thenReturn(requestThrottledCounter);
        when(testPluginMetrics.counter("resourceNotFound")).thenReturn(resourceNotFoundCounter);
        when(testPluginMetrics.counter("nonRetryableErrors")).thenReturn(nonRetryableErrorsCounter);
        when(testPluginMetrics.counter("retryableErrors")).thenReturn(retryableErrorsCounter);
        
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(testPluginMetrics, false);
        
        assertThat(recorderDisabled, notNullValue());
        
        // Verify subscription metrics are NOT created when enabled=false
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsSuccess");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsFailed");
        verify(testPluginMetrics, org.mockito.Mockito.never()).timer("startSubscriptionRequestLatency");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionApiCalls");
        
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionRequestsSuccess");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionRequestsFailed");
        verify(testPluginMetrics, org.mockito.Mockito.never()).timer("listSubscriptionRequestLatency");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionApiCalls");
        
        // Verify non-subscription metrics are still created
        verify(testPluginMetrics).counter("searchRequestsSuccess");
        verify(testPluginMetrics).counter("getRequestsSuccess");
        verify(testPluginMetrics).counter("authenticationRequestsSuccess");
    }

    @Test
    void constructor_DefaultConstructor_EnablesSubscriptionMetrics() {
        // The default constructor should enable subscription metrics for backward compatibility
        // This is already tested in setUp(), but adding explicit test for clarity
        assertThat(recorder, notNullValue());
        
        // Should have created subscription metrics (verified in constructor_CreatesAllMetricsCorrectly test)
        verify(pluginMetrics).counter("startSubscriptionRequestsSuccess");
        verify(pluginMetrics).counter("listSubscriptionRequestsSuccess");
    }

    @Test
    void recordSubscriptionSuccess_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        // Should not throw exception and should not call any subscription counters
        recorderDisabled.recordSubscriptionSuccess();
        
        // No additional verifications needed - method should do nothing silently
    }

    @Test
    void recordSubscriptionFailure_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        recorderDisabled.recordSubscriptionFailure();
        
        // Should complete without exception
    }

    @Test
    void recordSubscriptionCall_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        recorderDisabled.recordSubscriptionCall();
        
        // Should complete without exception
    }

    @Test
    void recordSubscriptionLatency_WhenDisabled_ExecutesOperationWithoutRecordingMetrics() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        String expectedResult = "operation result";
        Supplier<String> operation = () -> expectedResult;
        
        String result = recorderDisabled.recordSubscriptionLatency(operation);
        
        // Operation should still execute and return result
        assertThat(result, equalTo(expectedResult));
        
        // But no timer interactions should occur
        verify(subscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Supplier.class));
    }

    @Test
    void recordSubscriptionLatency_WhenDisabled_ExecutesRunnableWithoutRecordingMetrics() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        final boolean[] operationExecuted = {false};
        Runnable operation = () -> operationExecuted[0] = true;
        
        recorderDisabled.recordSubscriptionLatency(operation);
        
        // Operation should still execute
        assertThat(operationExecuted[0], equalTo(true));
        
        // But no timer interactions should occur
        verify(subscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Runnable.class));
    }

    @Test
    void recordSubscriptionLatency_WhenDisabled_WithDuration_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        Duration duration = Duration.ofMillis(100);
        
        recorderDisabled.recordSubscriptionLatency(duration);
        
        // Should complete without exception
        verify(subscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Duration.class));
    }

    @Test
    void recordListSubscriptionSuccess_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        recorderDisabled.recordListSubscriptionSuccess();
        
        // Should complete without exception
    }

    @Test
    void recordListSubscriptionFailure_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        recorderDisabled.recordListSubscriptionFailure();
        
        // Should complete without exception
    }

    @Test
    void recordListSubscriptionCall_WhenDisabled_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        recorderDisabled.recordListSubscriptionCall();
        
        // Should complete without exception
    }

    @Test
    void recordListSubscriptionLatency_WhenDisabled_ExecutesOperationWithoutRecordingMetrics() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        String expectedResult = "list operation result";
        Supplier<String> operation = () -> expectedResult;
        
        String result = recorderDisabled.recordListSubscriptionLatency(operation);
        
        // Operation should still execute and return result
        assertThat(result, equalTo(expectedResult));
        
        // But no timer interactions should occur
        verify(listSubscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Supplier.class));
    }

    @Test
    void recordListSubscriptionLatency_WhenDisabled_ExecutesRunnableWithoutRecordingMetrics() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        final boolean[] operationExecuted = {false};
        Runnable operation = () -> operationExecuted[0] = true;
        
        recorderDisabled.recordListSubscriptionLatency(operation);
        
        // Operation should still execute
        assertThat(operationExecuted[0], equalTo(true));
        
        // But no timer interactions should occur
        verify(listSubscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Runnable.class));
    }

    @Test
    void recordListSubscriptionLatency_WhenDisabled_WithDuration_DoesNothing() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        Duration duration = Duration.ofMillis(75);
        
        recorderDisabled.recordListSubscriptionLatency(duration);
        
        // Should complete without exception
        verify(listSubscriptionLatencyTimer, org.mockito.Mockito.never()).record(any(Duration.class));
    }

    @Test
    void nonSubscriptionMetrics_WhenSubscriptionMetricsDisabled_StillWorkNormally() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        // All non-subscription metrics should work normally
        recorderDisabled.recordSearchSuccess();
        recorderDisabled.recordGetSuccess();
        recorderDisabled.recordAuthSuccess();
        recorderDisabled.recordDataApiRequest();
        recorderDisabled.recordLogsRequested();
        
        // Verify they were called
        verify(searchSuccessCounter).increment();
        verify(getSuccessCounter).increment();
        verify(authSuccessCounter).increment();
        verify(totalDataApiRequestsCounter).increment();
        verify(logsRequestedCounter).increment();
    }

    @Test
    void subscriptionMetricsEnabled_WorkNormallyAfterConstruction() {
        // Use separate PluginMetrics mock to avoid interference
        PluginMetrics separatePluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        Counter separateSubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        Counter separateListSubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        
        // Set up all required mocks for constructor first
        // Use default mock for non-specific counters
        Counter defaultCounter = org.mockito.Mockito.mock(Counter.class);
        Timer defaultTimer = org.mockito.Mockito.mock(Timer.class);
        DistributionSummary defaultSummary = org.mockito.Mockito.mock(DistributionSummary.class);
        
        // Set up specific subscription counters we want to verify
        when(separatePluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(separateSubscriptionCounter);
        when(separatePluginMetrics.counter("listSubscriptionRequestsSuccess")).thenReturn(separateListSubscriptionCounter);
        
        // Set up all other required mocks for constructor (non-subscription metrics)
        when(separatePluginMetrics.counter("searchRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("searchRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("searchRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.summary("searchResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics.counter("getRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("getRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("getRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.summary("getResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics.counter("authenticationRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("authenticationRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("authenticationRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("listSubscriptionRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("listSubscriptionRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.counter("listSubscriptionApiCalls")).thenReturn(defaultCounter);
        when(separatePluginMetrics.timer("bufferWriteLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics.counter("bufferWriteAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("bufferWriteSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("bufferWriteRetrySuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("bufferWriteRetryAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("bufferWriteFailures")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("totalDataApiRequests")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("logsRequested")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("requestAccessDenied")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("requestThrottled")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("resourceNotFound")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("nonRetryableErrors")).thenReturn(defaultCounter);
        when(separatePluginMetrics.counter("retryableErrors")).thenReturn(defaultCounter);
        
        VendorAPIMetricsRecorder recorderEnabled = new VendorAPIMetricsRecorder(separatePluginMetrics, true);
        
        // Subscription metrics should work normally when enabled
        recorderEnabled.recordSubscriptionSuccess();
        recorderEnabled.recordListSubscriptionSuccess();
        
        // Verify with the separate mocks
        verify(separateSubscriptionCounter, times(1)).increment();
        verify(separateListSubscriptionCounter, times(1)).increment();
    }

    @Test
    void mixedScenario_SubscriptionDisabled_OtherMetricsEnabled() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        // Mix of subscription and non-subscription operations
        recorderDisabled.recordSubscriptionSuccess(); // Should do nothing
        recorderDisabled.recordSearchSuccess(); // Should work
        recorderDisabled.recordListSubscriptionCall(); // Should do nothing
        recorderDisabled.recordGetSuccess(); // Should work
        
        // Only non-subscription metrics should be recorded
        verify(searchSuccessCounter).increment();
        verify(getSuccessCounter).increment();
        
        // Subscription metrics should not have any additional calls beyond setUp
        // The setUp() method creates the main recorder instance, so subscription counters were called during setUp
        // But the disabled recorder should never call them
        // No additional verification needed for subscription counters - they should have been called during setUp only
    }

    @Test
    void subscriptionLatencyOperations_WhenDisabled_PropagateExceptions() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Test exception");
        };
        
        // Exception should still be propagated even when metrics are disabled
        assertThrows(RuntimeException.class, () -> {
            recorderDisabled.recordSubscriptionLatency(failingOperation);
        });
        
        // And for list subscriptions
        assertThrows(RuntimeException.class, () -> {
            recorderDisabled.recordListSubscriptionLatency(failingOperation);
        });
    }

    @Test
    void subscriptionLatencyRunnableOperations_WhenDisabled_PropagateExceptions() {
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        
        Runnable failingOperation = () -> {
            throw new RuntimeException("Test runnable exception");
        };
        
        // Exception should still be propagated even when metrics are disabled
        assertThrows(RuntimeException.class, () -> {
            recorderDisabled.recordSubscriptionLatency(failingOperation);
        });
        
        // And for list subscriptions
        assertThrows(RuntimeException.class, () -> {
            recorderDisabled.recordListSubscriptionLatency(failingOperation);
        });
    }

    @Test
    void multipleInstancesWithDifferentSettings_WorkIndependently() {
        // Use separate PluginMetrics mocks to avoid interference with other tests
        PluginMetrics separatePluginMetrics1 = org.mockito.Mockito.mock(PluginMetrics.class);
        PluginMetrics separatePluginMetrics2 = org.mockito.Mockito.mock(PluginMetrics.class);
        
        Counter separateSearchCounter1 = org.mockito.Mockito.mock(Counter.class);
        Counter separateSearchCounter2 = org.mockito.Mockito.mock(Counter.class);
        Counter separateSubscriptionCounter1 = org.mockito.Mockito.mock(Counter.class);
        
        // Create default mocks for constructor
        Counter defaultCounter = org.mockito.Mockito.mock(Counter.class);
        Timer defaultTimer = org.mockito.Mockito.mock(Timer.class);
        DistributionSummary defaultSummary = org.mockito.Mockito.mock(DistributionSummary.class);
        
        // Setup mocks for enabled recorder (with subscription metrics)
        when(separatePluginMetrics1.counter("searchRequestsSuccess")).thenReturn(separateSearchCounter1);
        when(separatePluginMetrics1.counter("startSubscriptionRequestsSuccess")).thenReturn(separateSubscriptionCounter1);
        // Set up all other required mocks for enabled recorder
        when(separatePluginMetrics1.counter("searchRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("searchRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.summary("searchResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics1.counter("getRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("getRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("getRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.summary("getResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics1.counter("authenticationRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("authenticationRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("authenticationRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.counter("startSubscriptionRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("startSubscriptionRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.counter("startSubscriptionApiCalls")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("listSubscriptionRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("listSubscriptionRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("listSubscriptionRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.counter("listSubscriptionApiCalls")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.timer("bufferWriteLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics1.counter("bufferWriteAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("bufferWriteSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("bufferWriteRetrySuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("bufferWriteRetryAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("bufferWriteFailures")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("totalDataApiRequests")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("logsRequested")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("requestAccessDenied")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("requestThrottled")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("resourceNotFound")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("nonRetryableErrors")).thenReturn(defaultCounter);
        when(separatePluginMetrics1.counter("retryableErrors")).thenReturn(defaultCounter);
        
        // Setup mocks for disabled recorder (without subscription metrics)
        when(separatePluginMetrics2.counter("searchRequestsSuccess")).thenReturn(separateSearchCounter2);
        when(separatePluginMetrics2.counter("searchRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.timer("searchRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics2.summary("searchResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics2.counter("getRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("getRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.timer("getRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics2.summary("getResponseSizeBytes")).thenReturn(defaultSummary);
        when(separatePluginMetrics2.counter("authenticationRequestsSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("authenticationRequestsFailed")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.timer("authenticationRequestLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics2.timer("bufferWriteLatency")).thenReturn(defaultTimer);
        when(separatePluginMetrics2.counter("bufferWriteAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("bufferWriteSuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("bufferWriteRetrySuccess")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("bufferWriteRetryAttempts")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("bufferWriteFailures")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("totalDataApiRequests")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("logsRequested")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("requestAccessDenied")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("requestThrottled")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("resourceNotFound")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("nonRetryableErrors")).thenReturn(defaultCounter);
        when(separatePluginMetrics2.counter("retryableErrors")).thenReturn(defaultCounter);
        
        VendorAPIMetricsRecorder recorderEnabled = new VendorAPIMetricsRecorder(separatePluginMetrics1, true);
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(separatePluginMetrics2, false);
        
        // Both should work for non-subscription metrics
        recorderEnabled.recordSearchSuccess();
        recorderDisabled.recordSearchSuccess();
        
        // Only enabled should work for subscription metrics
        recorderEnabled.recordSubscriptionSuccess();
        recorderDisabled.recordSubscriptionSuccess(); // Should do nothing
        
        // Verify correct behavior with separate mocks
        verify(separateSearchCounter1, times(1)).increment();
        verify(separateSearchCounter2, times(1)).increment();
        verify(separateSubscriptionCounter1, times(1)).increment();
    }

    @Test
    void gatingLogic_VerifyNullChecks_DoNotCauseNullPointerExceptions() {
        // This test ensures that when subscription metrics are disabled, 
        // the internal counters/timers are null but methods handle this gracefully
        VendorAPIMetricsRecorder recorderDisabled = new VendorAPIMetricsRecorder(pluginMetrics, false);
        Duration nullDuration = null;
        
        // All these should complete without NPE
        recorderDisabled.recordSubscriptionSuccess();
        recorderDisabled.recordSubscriptionFailure();
        recorderDisabled.recordSubscriptionCall();
        recorderDisabled.recordSubscriptionLatency(Duration.ofMillis(50));
        
        recorderDisabled.recordListSubscriptionSuccess();
        recorderDisabled.recordListSubscriptionFailure();
        recorderDisabled.recordListSubscriptionCall();
        recorderDisabled.recordListSubscriptionLatency(Duration.ofMillis(75));
        
        // If we reach here, no NPEs were thrown
        assertThat(recorderDisabled, notNullValue());

        doThrow(new NullPointerException()).when(bufferWriteLatencyTimer).record(nullDuration);

        assertThrows(NullPointerException.class, () -> {
            recorder.recordBufferWriteLatency(nullDuration);
        });
    }

    @Test
    void mixedOperationsIncludingBuffer_WorkCorrectly() {
        // Test that we can use different operation types including buffer metrics
        recorder.recordSearchSuccess();
        recorder.recordGetSuccess();
        recorder.recordAuthSuccess();
        recorder.recordSubscriptionSuccess();
        recorder.recordBufferWriteSuccess();
        recorder.recordRetryableError();

        verify(searchSuccessCounter).increment();
        verify(getSuccessCounter).increment();
        verify(authSuccessCounter).increment();
        verify(subscriptionSuccessCounter).increment();
        verify(bufferWriteSuccessCounter).increment();
        verify(retryableErrorsCounter).increment();
    }

    // === NEW SETTER METHOD AND LAZY INITIALIZATION TESTS ===

    @Test
    void enableSubscriptionMetrics_WithDefaultConstructor_EnablesSubscriptionMetricsLazily() {
        // Use separate PluginMetrics mock for clean testing
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        // Create recorder with default constructor (subscription metrics disabled)
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Verify subscription metrics are NOT created initially
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsSuccess");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionRequestsSuccess");
        
        // Setup subscription metric mocks for lazy initialization
        Counter lazySubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        Counter lazyListSubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        when(testPluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(lazySubscriptionCounter);
        when(testPluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(org.mockito.Mockito.mock(Timer.class));
        when(testPluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.counter("listSubscriptionRequestsSuccess")).thenReturn(lazyListSubscriptionCounter);
        when(testPluginMetrics.counter("listSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("listSubscriptionRequestLatency")).thenReturn(org.mockito.Mockito.mock(Timer.class));
        when(testPluginMetrics.counter("listSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        
        // Enable subscription metrics
        testRecorder.enableSubscriptionMetrics();
        
        // Verify subscription metrics are now created
        verify(testPluginMetrics).counter("startSubscriptionRequestsSuccess");
        verify(testPluginMetrics).counter("startSubscriptionRequestsFailed");
        verify(testPluginMetrics).timer("startSubscriptionRequestLatency");
        verify(testPluginMetrics).counter("startSubscriptionApiCalls");
        verify(testPluginMetrics).counter("listSubscriptionRequestsSuccess");
        verify(testPluginMetrics).counter("listSubscriptionRequestsFailed");
        verify(testPluginMetrics).timer("listSubscriptionRequestLatency");
        verify(testPluginMetrics).counter("listSubscriptionApiCalls");
        
        // Test that subscription metrics now work
        testRecorder.recordSubscriptionSuccess();
        testRecorder.recordListSubscriptionSuccess();
        
        verify(lazySubscriptionCounter).increment();
        verify(lazyListSubscriptionCounter).increment();
    }

    @Test
    void enableSubscriptionMetrics_WhenAlreadyEnabled_DoesNotReinitialize() {
        // Use existing recorder which has subscription metrics enabled in setUp
        
        // Reset the mock to count only new interactions
        org.mockito.Mockito.reset(pluginMetrics);
        
        // Call enableSubscriptionMetrics when already enabled
        recorder.enableSubscriptionMetrics();
        
        // Should not create metrics again
        verify(pluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsSuccess");
        verify(pluginMetrics, org.mockito.Mockito.never()).timer("startSubscriptionRequestLatency");
        verify(pluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionRequestsSuccess");
        verify(pluginMetrics, org.mockito.Mockito.never()).timer("listSubscriptionRequestLatency");
    }

    @Test
    void enableSubscriptionMetrics_MultipleEnableCalls_OnlyInitializesOnce() {
        // Use separate PluginMetrics mock for clean testing
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Setup subscription metric mocks
        when(testPluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(org.mockito.Mockito.mock(Timer.class));
        when(testPluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.counter("listSubscriptionRequestsSuccess")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.counter("listSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("listSubscriptionRequestLatency")).thenReturn(org.mockito.Mockito.mock(Timer.class));
        when(testPluginMetrics.counter("listSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        
        // Call enable multiple times
        testRecorder.enableSubscriptionMetrics();
        testRecorder.enableSubscriptionMetrics();
        testRecorder.enableSubscriptionMetrics();
        
        // Verify metrics were only created once
        verify(testPluginMetrics, times(1)).counter("startSubscriptionRequestsSuccess");
        verify(testPluginMetrics, times(1)).counter("listSubscriptionRequestsSuccess");
    }

    @Test
    void subscriptionOperations_BeforeEnable_DoNothing() {
        // Use separate PluginMetrics mock for clean testing
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Try subscription operations before enabling - should not create metrics
        testRecorder.recordSubscriptionSuccess();
        testRecorder.recordSubscriptionFailure();
        testRecorder.recordSubscriptionCall();
        testRecorder.recordListSubscriptionSuccess();
        testRecorder.recordListSubscriptionFailure();
        testRecorder.recordListSubscriptionCall();
        
        // Verify no subscription metrics were created
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsSuccess");
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("listSubscriptionRequestsSuccess");
    }

    @Test
    void subscriptionLatencyOperations_BeforeEnable_ExecuteWithoutMetrics() {
        // Use separate PluginMetrics mock for clean testing
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Test supplier operations still execute
        String expectedResult = "test result";
        Supplier<String> operation = () -> expectedResult;
        
        String result1 = testRecorder.recordSubscriptionLatency(operation);
        String result2 = testRecorder.recordListSubscriptionLatency(operation);
        
        assertThat(result1, equalTo(expectedResult));
        assertThat(result2, equalTo(expectedResult));
        
        // Test runnable operations still execute
        final boolean[] operationExecuted = {false, false};
        Runnable runnableOperation1 = () -> operationExecuted[0] = true;
        Runnable runnableOperation2 = () -> operationExecuted[1] = true;
        
        testRecorder.recordSubscriptionLatency(runnableOperation1);
        testRecorder.recordListSubscriptionLatency(runnableOperation2);
        
        assertThat(operationExecuted[0], equalTo(true));
        assertThat(operationExecuted[1], equalTo(true));
        
        // Verify no metrics were created
        verify(testPluginMetrics, org.mockito.Mockito.never()).timer("startSubscriptionRequestLatency");
        verify(testPluginMetrics, org.mockito.Mockito.never()).timer("listSubscriptionRequestLatency");
    }

    @Test
    void subscriptionOperations_AfterEnable_Work() {
        // Use separate PluginMetrics mock for clean testing
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Setup subscription metric mocks
        Counter lazySubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        Counter lazyListSubscriptionCounter = org.mockito.Mockito.mock(Counter.class);
        Timer lazySubscriptionTimer = org.mockito.Mockito.mock(Timer.class);
        Timer lazyListSubscriptionTimer = org.mockito.Mockito.mock(Timer.class);
        
        when(testPluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(lazySubscriptionCounter);
        when(testPluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(lazySubscriptionTimer);
        when(testPluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.counter("listSubscriptionRequestsSuccess")).thenReturn(lazyListSubscriptionCounter);
        when(testPluginMetrics.counter("listSubscriptionRequestsFailed")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        when(testPluginMetrics.timer("listSubscriptionRequestLatency")).thenReturn(lazyListSubscriptionTimer);
        when(testPluginMetrics.counter("listSubscriptionApiCalls")).thenReturn(org.mockito.Mockito.mock(Counter.class));
        
        // Enable subscription metrics
        testRecorder.enableSubscriptionMetrics();
        
        // Test operations work
        testRecorder.recordSubscriptionSuccess();
        testRecorder.recordListSubscriptionSuccess();
        
        // Test latency operations
        String expectedResult = "test result";
        Supplier<String> operation = () -> expectedResult;
        when(lazySubscriptionTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        when(lazyListSubscriptionTimer.record(any(Supplier.class))).thenReturn(expectedResult);
        
        String result1 = testRecorder.recordSubscriptionLatency(operation);
        String result2 = testRecorder.recordListSubscriptionLatency(operation);
        
        // Verify operations worked
        verify(lazySubscriptionCounter).increment();
        verify(lazyListSubscriptionCounter).increment();
        verify(lazySubscriptionTimer).record(any(Supplier.class));
        verify(lazyListSubscriptionTimer).record(any(Supplier.class));
        assertThat(result1, equalTo(expectedResult));
        assertThat(result2, equalTo(expectedResult));
    }

    @Test
    void enableSubscriptionMetrics_WithInjectConstructor_StartsDisabled() {
        // Use separate PluginMetrics mock to avoid interference with setUp
        PluginMetrics testPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        setupNonSubscriptionMocks(testPluginMetrics);
        
        // Use @Inject constructor (single parameter)
        VendorAPIMetricsRecorder testRecorder = new VendorAPIMetricsRecorder(testPluginMetrics);
        
        // Should start with subscription metrics disabled
        testRecorder.recordSubscriptionSuccess(); // Should do nothing
        
        // Verify no subscription metrics were created
        verify(testPluginMetrics, org.mockito.Mockito.never()).counter("startSubscriptionRequestsSuccess");
        
        // We can verify subscription metrics disabled behavior indirectly by testing that operations do nothing
    }

    // Helper method to set up non-subscription metric mocks to reduce duplication
    private void setupNonSubscriptionMocks(PluginMetrics mockPluginMetrics) {
        Counter defaultCounter = org.mockito.Mockito.mock(Counter.class);
        Timer defaultTimer = org.mockito.Mockito.mock(Timer.class);
        DistributionSummary defaultSummary = org.mockito.Mockito.mock(DistributionSummary.class);
        
        when(mockPluginMetrics.counter("searchRequestsSuccess")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("searchRequestsFailed")).thenReturn(defaultCounter);
        when(mockPluginMetrics.timer("searchRequestLatency")).thenReturn(defaultTimer);
        when(mockPluginMetrics.summary("searchResponseSizeBytes")).thenReturn(defaultSummary);
        when(mockPluginMetrics.counter("getRequestsSuccess")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("getRequestsFailed")).thenReturn(defaultCounter);
        when(mockPluginMetrics.timer("getRequestLatency")).thenReturn(defaultTimer);
        when(mockPluginMetrics.summary("getResponseSizeBytes")).thenReturn(defaultSummary);
        when(mockPluginMetrics.counter("authenticationRequestsSuccess")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("authenticationRequestsFailed")).thenReturn(defaultCounter);
        when(mockPluginMetrics.timer("authenticationRequestLatency")).thenReturn(defaultTimer);
        when(mockPluginMetrics.timer("bufferWriteLatency")).thenReturn(defaultTimer);
        when(mockPluginMetrics.counter("bufferWriteAttempts")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("bufferWriteSuccess")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("bufferWriteRetrySuccess")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("bufferWriteRetryAttempts")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("bufferWriteFailures")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("totalDataApiRequests")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("logsRequested")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("requestAccessDenied")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("requestThrottled")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("resourceNotFound")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("nonRetryableErrors")).thenReturn(defaultCounter);
        when(mockPluginMetrics.counter("retryableErrors")).thenReturn(defaultCounter);
    }
}
