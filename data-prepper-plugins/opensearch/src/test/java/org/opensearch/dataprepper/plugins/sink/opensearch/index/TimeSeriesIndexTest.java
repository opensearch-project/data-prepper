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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.OperationType;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.ScriptManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimeSeriesIndexTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private TimeSeriesIndexDetector timeSeriesIndexDetector;
    @Mock
    private Event event;
    @Mock
    private EventHandle eventHandle;

    private BulkOperationFactory bulkOperationFactory;
    private boolean usingDocumentFilters;
    private String indexName;
    private String timestampField;

    @BeforeEach
    void setUp() {
        bulkOperationFactory = new BulkOperationFactory(null, mock(ScriptManager.class), new ObjectMapper(), false);
        usingDocumentFilters = false;
        indexName = UUID.randomUUID().toString();
        timestampField = UUID.randomUUID().toString();
    }

    private TimeSeriesIndex createObjectUnderTest() {
        return new TimeSeriesIndex(timeSeriesIndexDetector, bulkOperationFactory, usingDocumentFilters);
    }

    @Test
    void ensureTimestamp_returns_document_for_non_time_series_index() {
        final SerializedJson document = createDocument("{}");
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.empty());

        assertThat(createObjectUnderTest().ensureTimestamp(event, document, indexName), sameInstance(document));
    }

    @Test
    void ensureTimestamp_returns_document_when_event_has_timestamp_field() {
        final SerializedJson document = createDocument("{}");
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.containsKey(timestampField)).thenReturn(true);

        assertThat(createObjectUnderTest().ensureTimestamp(event, document, indexName), sameInstance(document));
    }

    @Test
    void ensureTimestamp_moves_default_timestamp_to_timestamp_field_without_modifying_event() throws IOException {
        final String timestamp = UUID.randomUUID().toString();
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.containsKey(timestampField)).thenReturn(false);

        final JsonNode json = toJson(createObjectUnderTest().ensureTimestamp(
                event, createDocument("{\"@timestamp\":\"" + timestamp + "\"}"), indexName));

        assertThat(json.get(timestampField).textValue(), equalTo(timestamp));
        assertThat(json.has("@timestamp"), equalTo(false));
        verify(event, never()).put(anyString(), any());
        verify(event, never()).delete(anyString());
    }

    @Test
    void ensureTimestamp_sets_timestamp_field_from_event_time() throws IOException {
        final Instant originationTime = Instant.now();
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.containsKey(timestampField)).thenReturn(false);
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getInternalOriginationTime()).thenReturn(originationTime);

        final JsonNode json = toJson(createObjectUnderTest().ensureTimestamp(event, createDocument("{}"), indexName));

        assertThat(json.get(timestampField).longValue(), equalTo(originationTime.toEpochMilli()));
    }

    @Test
    void ensureTimestamp_uses_event_time_when_default_timestamp_is_null() throws IOException {
        final Instant originationTime = Instant.now();
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.containsKey(timestampField)).thenReturn(false);
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getInternalOriginationTime()).thenReturn(originationTime);

        final JsonNode json = toJson(createObjectUnderTest().ensureTimestamp(
                event, createDocument("{\"@timestamp\":null}"), indexName));

        assertThat(json.get(timestampField).longValue(), equalTo(originationTime.toEpochMilli()));
    }

    @Test
    void ensureTimestamp_checks_document_instead_of_event_when_using_document_filters() throws IOException {
        usingDocumentFilters = true;
        final Instant originationTime = Instant.now();
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getInternalOriginationTime()).thenReturn(originationTime);

        final JsonNode json = toJson(createObjectUnderTest().ensureTimestamp(event, createDocument("{}"), indexName));

        assertThat(json.get(timestampField).longValue(), equalTo(originationTime.toEpochMilli()));
        verify(event, never()).containsKey(anyString());
    }

    @Test
    void ensureTimestamp_keeps_routing_and_pipeline() {
        final String routing = UUID.randomUUID().toString();
        final String pipeline = UUID.randomUUID().toString();
        final SerializedJson document = SerializedJson.builder().withJsonString("{\"@timestamp\":\"t\"}")
                .withRoutingField(routing).withPipelineField(pipeline).build();
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));
        when(event.containsKey(timestampField)).thenReturn(false);

        final SerializedJson result = createObjectUnderTest().ensureTimestamp(event, document, indexName);

        assertThat(result.getRoutingField(), equalTo(Optional.of(routing)));
        assertThat(result.getPipelineField(), equalTo(Optional.of(pipeline)));
    }

    @Test
    void isMissingTimestampError_returns_true_for_bad_request_with_missing_timestamp_error() {
        final BulkResponseItem bulkResponseItem = createFailedItem(indexName, 400);
        when(timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem)).thenReturn(Optional.of(timestampField));

        assertThat(createObjectUnderTest().isMissingTimestampError(bulkResponseItem), equalTo(true));
    }

    @Test
    void isMissingTimestampError_returns_false_for_other_bad_request() {
        final BulkResponseItem bulkResponseItem = createFailedItem(indexName, 400);
        when(timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem)).thenReturn(Optional.empty());

        assertThat(createObjectUnderTest().isMissingTimestampError(bulkResponseItem), equalTo(false));
    }

    @Test
    void isMissingTimestampError_returns_false_without_reading_error_for_other_status() {
        assertThat(createObjectUnderTest().isMissingTimestampError(createFailedItem(indexName, 429)), equalTo(false));

        verify(timeSeriesIndexDetector, never()).getMissingTimestampField(any());
    }

    @Test
    void addMissingTimestamp_returns_create_operation_with_timestamp_field_and_records_field() throws IOException {
        final Instant originationTime = Instant.now();
        when(eventHandle.getInternalOriginationTime()).thenReturn(originationTime);
        final BulkResponseItem bulkResponseItem = createMissingTimestampItem();
        final BulkOperationWrapper operation = new BulkOperationWrapper(createCreateOperation("{\"message\":\"m\"}"), eventHandle);

        final BulkOperationWrapper result = createObjectUnderTest().addMissingTimestamp(operation, bulkResponseItem);

        assertThat(result.getBulkOperation().isCreate(), equalTo(true));
        assertThat(result.getIndex(), equalTo(indexName));
        assertThat(result.getEventHandle(), sameInstance(eventHandle));
        final JsonNode json = toJson((SerializedJson) result.getDocument());
        assertThat(json.get(timestampField).longValue(), equalTo(originationTime.toEpochMilli()));
        assertThat(json.get("message").textValue(), equalTo("m"));
        verify(timeSeriesIndexDetector).recordTimestampField(indexName, timestampField);
    }

    @Test
    void addMissingTimestamp_returns_null_when_document_has_timestamp_field() {
        final BulkResponseItem bulkResponseItem = createMissingTimestampItem();
        final BulkOperationWrapper operation = new BulkOperationWrapper(
                createCreateOperation("{\"" + timestampField + "\":\"t\"}"), eventHandle);

        assertThat(createObjectUnderTest().addMissingTimestamp(operation, bulkResponseItem), nullValue());
    }

    @Test
    void addMissingTimestamp_returns_null_for_delete_operation() {
        final BulkResponseItem bulkResponseItem = createMissingTimestampItem();
        final BulkOperation deleteOperation = new BulkOperation.Builder()
                .delete(new DeleteOperation.Builder().index(indexName).id(UUID.randomUUID().toString()).build()).build();

        assertThat(createObjectUnderTest().addMissingTimestamp(new BulkOperationWrapper(deleteOperation, eventHandle), bulkResponseItem),
                nullValue());
        verify(timeSeriesIndexDetector, never()).recordTimestampField(anyString(), anyString());
    }

    @Test
    void addMissingTimestamp_returns_null_for_other_error() {
        final BulkResponseItem bulkResponseItem = createFailedItem(indexName, 400);
        when(timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem)).thenReturn(Optional.empty());
        final BulkOperationWrapper operation = new BulkOperationWrapper(createCreateOperation("{}"), eventHandle);

        assertThat(createObjectUnderTest().addMissingTimestamp(operation, bulkResponseItem), nullValue());
    }

    @Test
    void addMissingTimestamp_returns_index_operation_with_version_for_index_operation() throws IOException {
        final Instant originationTime = Instant.now();
        when(eventHandle.getInternalOriginationTime()).thenReturn(originationTime);
        final long version = new Random().nextInt(1000) + 1;
        final BulkOperation indexOperation = new BulkOperation.Builder()
                .index(new IndexOperation.Builder<Object>().index(indexName).version(version).document(createDocument("{}")).build())
                .build();

        final BulkOperationWrapper result = createObjectUnderTest().addMissingTimestamp(
                new BulkOperationWrapper(indexOperation, eventHandle), createMissingTimestampItem());

        assertThat(result.getBulkOperation().isIndex(), equalTo(true));
        assertThat(result.getBulkOperation().index().version(), equalTo(version));
        assertThat(toJson((SerializedJson) result.getDocument()).get(timestampField).longValue(), equalTo(originationTime.toEpochMilli()));
    }

    @Test
    void addMissingTimestamp_keeps_event_of_operation() {
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        final BulkOperationWrapper operation = new BulkOperationWrapper(createCreateOperation("{}"), event, null, null);

        final BulkOperationWrapper result = createObjectUnderTest().addMissingTimestamp(operation, createMissingTimestampItem());

        assertThat(result.getEvent(), sameInstance(event));
    }

    @Test
    void addMissingTimestamp_returns_null_when_document_is_not_an_object() {
        final BulkOperationWrapper operation = new BulkOperationWrapper(createCreateOperation("\"" + UUID.randomUUID() + "\""), eventHandle);

        assertThat(createObjectUnderTest().addMissingTimestamp(operation, createMissingTimestampItem()), nullValue());
    }

    @Test
    void ensureTimestamp_returns_document_when_document_is_not_an_object() {
        usingDocumentFilters = true;
        final SerializedJson document = createDocument("[" + new Random().nextInt() + "]");
        when(timeSeriesIndexDetector.getTimestampField(indexName)).thenReturn(Optional.of(timestampField));

        assertThat(createObjectUnderTest().ensureTimestamp(event, document, indexName), sameInstance(document));
    }

    @Test
    void addMissingTimestamp_keeps_document_id() {
        final String documentId = UUID.randomUUID().toString();
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        final BulkOperation createOperation = new BulkOperation.Builder()
                .create(new CreateOperation.Builder<Object>().index(indexName).id(documentId)
                        .document(SerializedJson.builder().withJsonString("{}").withDocumentId(documentId).build()).build())
                .build();

        final BulkOperationWrapper result = createObjectUnderTest().addMissingTimestamp(
                new BulkOperationWrapper(createOperation, eventHandle), createMissingTimestampItem());

        assertThat(result.getId(), equalTo(documentId));
    }

    private BulkResponseItem createMissingTimestampItem() {
        final BulkResponseItem bulkResponseItem = createFailedItem(indexName, 400);
        when(timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem)).thenReturn(Optional.of(timestampField));
        return bulkResponseItem;
    }

    private BulkOperation createCreateOperation(final String json) {
        return new BulkOperation.Builder()
                .create(new CreateOperation.Builder<Object>().index(indexName).document(createDocument(json)).build())
                .build();
    }

    private static BulkResponseItem createFailedItem(final String index, final int status) {
        return BulkResponseItem.of(item -> item
                .index(index)
                .status(status)
                .operationType(OperationType.Create)
                .error(error -> error.type("illegal_argument_exception").reason(UUID.randomUUID().toString())));
    }

    private static SerializedJson createDocument(final String json) {
        return SerializedJson.builder().withJsonString(json).build();
    }

    private static JsonNode toJson(final SerializedJson document) throws IOException {
        return OBJECT_MAPPER.readTree(document.getSerializedJson());
    }
}
