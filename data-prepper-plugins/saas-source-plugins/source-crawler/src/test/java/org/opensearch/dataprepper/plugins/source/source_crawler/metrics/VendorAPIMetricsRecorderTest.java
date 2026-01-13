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

    // Subscription metrics
    @Mock
    private Counter subscriptionSuccessCounter;
    @Mock
    private Counter subscriptionFailureCounter;
    @Mock
    private Timer subscriptionLatencyTimer;
    @Mock
    private Counter subscriptionCallsCounter;

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

        // Setup subscription metrics mocks
        when(pluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(subscriptionSuccessCounter);
        when(pluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(subscriptionFailureCounter);
        when(pluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(subscriptionLatencyTimer);
        when(pluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(subscriptionCallsCounter);

        // Setup shared metrics mocks
        when(pluginMetrics.counter("totalDataApiRequests")).thenReturn(totalDataApiRequestsCounter);
        when(pluginMetrics.counter("logsRequested")).thenReturn(logsRequestedCounter);

        // Setup error metrics mocks
        when(pluginMetrics.counter("requestAccessDenied")).thenReturn(requestAccessDeniedCounter);
        when(pluginMetrics.counter("requestThrottled")).thenReturn(requestThrottledCounter);
        when(pluginMetrics.counter("resourceNotFound")).thenReturn(resourceNotFoundCounter);

        recorder = new VendorAPIMetricsRecorder(pluginMetrics);
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

        // Verify subscription metrics creation
        verify(pluginMetrics).counter("startSubscriptionRequestsSuccess");
        verify(pluginMetrics).counter("startSubscriptionRequestsFailed");
        verify(pluginMetrics).timer("startSubscriptionRequestLatency");
        verify(pluginMetrics).counter("startSubscriptionApiCalls");

        // Verify shared metrics creation
        verify(pluginMetrics).counter("totalDataApiRequests");
        verify(pluginMetrics).counter("logsRequested");
        
        // Verify error metrics creation
        verify(pluginMetrics).counter("requestAccessDenied");
        verify(pluginMetrics).counter("requestThrottled");
        verify(pluginMetrics).counter("resourceNotFound");
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
}
