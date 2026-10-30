/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.OperationType;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ComponentTemplateAPIWrapperTest.toJsonData;

@ExtendWith(MockitoExtension.class)
class TimeSeriesIndexDetectorTest {
    @Mock
    private OpenSearchClient openSearchClient;
    @Mock
    private OpenSearchTransport openSearchTransport;
    @Mock
    private TransportOptions openSearchTransportOptions;
    @Captor
    private ArgumentCaptor<Endpoint<Void, JsonData, ErrorResponse>> endpointCaptor;

    private IndexCache indexCache;
    private String indexName;
    private String timestampField;

    @BeforeEach
    void setUp() {
        indexCache = new IndexCache();
        indexName = UUID.randomUUID().toString();
        timestampField = UUID.randomUUID().toString();
    }

    private TimeSeriesIndexDetector createObjectUnderTest() {
        return new TimeSeriesIndexDetector(openSearchClient, indexCache);
    }

    @Test
    void getTimestampField_returns_timestamp_field_when_setting_is_present() throws IOException {
        stubResponse("{\"" + indexName + "\":{\"settings\":{\"index.timestamp_field\":\"" + timestampField + "\"}}}");

        assertThat(createObjectUnderTest().getTimestampField(indexName), equalTo(Optional.of(timestampField)));

        verify(openSearchTransport).performRequest(isNull(), endpointCaptor.capture(), eq(openSearchTransportOptions));
        final Endpoint<Void, JsonData, ErrorResponse> endpoint = endpointCaptor.getValue();
        assertThat(endpoint.method(null), equalTo("GET"));
        assertThat(endpoint.requestUrl(null), equalTo("/" + indexName + "/_settings/index.timestamp_field"));
        assertThat(endpoint.queryParameters(null), equalTo(Map.of("flat_settings", "true")));
        assertThat(endpoint.hasRequestBody(), equalTo(false));
    }

    @Test
    void getTimestampField_returns_empty_when_setting_is_absent() throws IOException {
        stubResponse("{\"" + indexName + "\":{\"settings\":{}}}");

        assertThat(createObjectUnderTest().getTimestampField(indexName), equalTo(Optional.empty()));
    }

    @Test
    void getTimestampField_returns_empty_when_response_is_empty() throws IOException {
        stubResponse("{}");

        assertThat(createObjectUnderTest().getTimestampField(indexName), equalTo(Optional.empty()));
    }

    @Test
    void getTimestampField_does_not_retry_failed_lookup_until_failed_lookups_are_cleared() throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        when(openSearchTransport.performRequest(isNull(), any(), eq(openSearchTransportOptions)))
                .thenThrow(new IOException(UUID.randomUUID().toString()));
        final TimeSeriesIndexDetector objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.empty()));
        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.empty()));
        assertThat(indexCache.isTimeSeriesStatusKnown(indexName), equalTo(false));
        verify(openSearchTransport, times(1)).performRequest(isNull(), any(), eq(openSearchTransportOptions));

        objectUnderTest.clearFailedLookups();
        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.empty()));
        verify(openSearchTransport, times(2)).performRequest(isNull(), any(), eq(openSearchTransportOptions));
    }

    @Test
    void getTimestampField_caches_result() throws IOException {
        stubResponse("{\"" + indexName + "\":{\"settings\":{\"index.timestamp_field\":\"" + timestampField + "\"}}}");
        final TimeSeriesIndexDetector objectUnderTest = createObjectUnderTest();

        objectUnderTest.getTimestampField(indexName);
        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.of(timestampField)));

        verify(openSearchTransport, times(1)).performRequest(isNull(), any(), eq(openSearchTransportOptions));
    }

    @Test
    void getTimestampField_caches_non_time_series_result() throws IOException {
        stubResponse("{\"" + indexName + "\":{\"settings\":{}}}");
        final TimeSeriesIndexDetector objectUnderTest = createObjectUnderTest();

        objectUnderTest.getTimestampField(indexName);
        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.empty()));

        verify(openSearchTransport, times(1)).performRequest(isNull(), any(), eq(openSearchTransportOptions));
    }

    @Test
    void getMissingTimestampField_returns_field_from_missing_timestamp_error() {
        assertThat(createObjectUnderTest().getMissingTimestampField(createBulkResponseItem(
                "document is missing required timestamp field [" + timestampField + "]")), equalTo(Optional.of(timestampField)));
    }

    @Test
    void getMissingTimestampField_returns_empty_for_other_errors() {
        assertThat(createObjectUnderTest().getMissingTimestampField(createBulkResponseItem(UUID.randomUUID().toString())),
                equalTo(Optional.empty()));
    }

    @Test
    void getMissingTimestampField_returns_empty_without_error() {
        final BulkResponseItem bulkResponseItem = BulkResponseItem.of(item -> item
                .index(indexName)
                .status(201)
                .operationType(OperationType.Create));

        assertThat(createObjectUnderTest().getMissingTimestampField(bulkResponseItem), equalTo(Optional.empty()));
    }

    @Test
    void recordTimestampField_caches_field_and_clears_failed_lookup() throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        when(openSearchTransport.performRequest(isNull(), any(), eq(openSearchTransportOptions)))
                .thenThrow(new IOException(UUID.randomUUID().toString()));
        final TimeSeriesIndexDetector objectUnderTest = createObjectUnderTest();
        objectUnderTest.getTimestampField(indexName);

        objectUnderTest.recordTimestampField(indexName, timestampField);

        assertThat(objectUnderTest.getTimestampField(indexName), equalTo(Optional.of(timestampField)));
        verify(openSearchTransport, times(1)).performRequest(isNull(), any(), eq(openSearchTransportOptions));
    }

    private BulkResponseItem createBulkResponseItem(final String reason) {
        return BulkResponseItem.of(item -> item
                .index(indexName)
                .status(400)
                .operationType(OperationType.Create)
                .error(error -> error.type("illegal_argument_exception").reason(reason)));
    }

    private void stubResponse(final String responseBody) throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        when(openSearchTransport.performRequest(isNull(), any(), eq(openSearchTransportOptions)))
                .thenReturn(toJsonData(responseBody));
    }
}
