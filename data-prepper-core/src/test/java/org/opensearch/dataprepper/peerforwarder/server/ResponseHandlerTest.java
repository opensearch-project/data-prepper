/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.SizeOverflowException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.peerforwarder.server.ResponseHandler.REQUEST_INTERNAL_SERVER_ERRORS;
import static org.opensearch.dataprepper.peerforwarder.server.ResponseHandler.REQUEST_TIMEOUTS;
import static org.opensearch.dataprepper.peerforwarder.server.ResponseHandler.REQUESTS_TOO_LARGE;

class ResponseHandlerTest {
    private static final String COMPONENT_SCOPE = "testComponentScope";
    private static final String COMPONENT_ID = "testComponentId";

    private PluginMetrics pluginMetrics;

    @BeforeAll
    static void beforeAll() {
        MetricsTestUtil.initMetrics();
    }

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
    }

    private ResponseHandler createObjectUnderTest() {
        return new ResponseHandler(pluginMetrics);
    }

    @Test
    void test_JsonProcessingException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(jsonProcessingException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        final List<Measurement> internalServerErrorMeasurementList = MetricsTestUtil.getMeasurementList(new StringJoiner
                (MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID).add(REQUEST_INTERNAL_SERVER_ERRORS).toString());
        final Measurement internalServerErrorMeasurement = MetricsTestUtil.getMeasurementFromList(internalServerErrorMeasurementList, Statistic.COUNT);
        assertThat(internalServerErrorMeasurement.getValue(), equalTo(1.0));
    }

    @Test
    void test_SizeOverflowException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final SizeOverflowException sizeOverflowException = mock(SizeOverflowException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(sizeOverflowException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.REQUEST_ENTITY_TOO_LARGE, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        final List<Measurement> requestsTooLargeMeasurementList = MetricsTestUtil.getMeasurementList(new StringJoiner
                (MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID).add(REQUESTS_TOO_LARGE).toString());
        final Measurement requestsTooLargeMeasurement = MetricsTestUtil.getMeasurementFromList(requestsTooLargeMeasurementList, Statistic.COUNT);
        assertThat(requestsTooLargeMeasurement.getValue(), equalTo(1.0));
    }

    @Test
    void test_TimeoutException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final TimeoutException timeoutException = mock(TimeoutException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(timeoutException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.REQUEST_TIMEOUT, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        final List<Measurement> requestTimeoutsMeasurementList = MetricsTestUtil.getMeasurementList(new StringJoiner
                (MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID).add(REQUEST_TIMEOUTS).toString());
        final Measurement requestTimeoutsMeasurement = MetricsTestUtil.getMeasurementFromList(requestTimeoutsMeasurementList, Statistic.COUNT);
        assertThat(requestTimeoutsMeasurement.getValue(), equalTo(1.0));
    }

    @Test
    void test_UnknownException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final UnknownException unknownException = new UnknownException("");

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(unknownException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());

        final List<Measurement> internalServerErrorMeasurementList = MetricsTestUtil.getMeasurementList(new StringJoiner
                (MetricNames.DELIMITER).add(COMPONENT_SCOPE).add(COMPONENT_ID).add(REQUEST_INTERNAL_SERVER_ERRORS).toString());
        final Measurement internalServerErrorMeasurement = MetricsTestUtil.getMeasurementFromList(internalServerErrorMeasurementList, Statistic.COUNT);
        assertThat(internalServerErrorMeasurement.getValue(), equalTo(2.0));
    }

    static class UnknownException extends Exception {
        public UnknownException(final String message) {
            super(message);
        }
    }

}