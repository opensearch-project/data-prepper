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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class TimeSeriesIndex {
    private static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";

    private final TimeSeriesIndexDetector timeSeriesIndexDetector;
    private final BulkOperationFactory bulkOperationFactory;
    private final boolean usingDocumentFilters;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TimeSeriesIndex(final TimeSeriesIndexDetector timeSeriesIndexDetector,
                           final BulkOperationFactory bulkOperationFactory,
                           final boolean usingDocumentFilters) {
        this.timeSeriesIndexDetector = timeSeriesIndexDetector;
        this.bulkOperationFactory = bulkOperationFactory;
        this.usingDocumentFilters = usingDocumentFilters;
    }

    /**
     * Adds the timestamp field to the document of a time series index. The event is not modified.
     *
     * @return the document with the timestamp field, or the given document if no change is needed
     */
    public SerializedJson ensureTimestamp(final Event event, final SerializedJson document, final String indexName) {
        final Optional<String> timestampField = timeSeriesIndexDetector.getTimestampField(indexName);
        if (timestampField.isEmpty() || (!usingDocumentFilters && event.containsKey(timestampField.get()))) {
            return document;
        }
        final SerializedJson documentWithTimestamp = addTimestamp(document, timestampField.get(), event.getEventHandle());
        return documentWithTimestamp != null ? documentWithTimestamp : document;
    }

    public boolean isMissingTimestampError(final BulkResponseItem bulkResponseItem) {
        return bulkResponseItem.status() == RestStatus.BAD_REQUEST.getStatus()
                && timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem).isPresent();
    }

    /**
     * @return a copy of the operation whose document has the timestamp field named by the bulk error, or null
     *         if the operation cannot be fixed because it is not a create or index operation or already has the field
     */
    public BulkOperationWrapper addMissingTimestamp(final BulkOperationWrapper operation, final BulkResponseItem bulkResponseItem) {
        final Optional<String> timestampField = timeSeriesIndexDetector.getMissingTimestampField(bulkResponseItem);
        final BulkOperation bulkOperation = operation.getBulkOperation();
        if (timestampField.isEmpty() || (!bulkOperation.isCreate() && !bulkOperation.isIndex())
                || !(operation.getDocument() instanceof SerializedJson)) {
            return null;
        }
        timeSeriesIndexDetector.recordTimestampField(bulkResponseItem.index(), timestampField.get());
        final SerializedJson documentWithTimestamp = addTimestamp(
                (SerializedJson) operation.getDocument(), timestampField.get(), operation.getEventHandle());
        if (documentWithTimestamp == null) {
            return null;
        }
        final String action = bulkOperation.isCreate() ? OpenSearchBulkActions.CREATE.toString() : OpenSearchBulkActions.INDEX.toString();
        final Long version = bulkOperation.isIndex() ? bulkOperation.index().version() : null;
        final BulkOperation bulkOperationWithTimestamp = bulkOperationFactory.create(
                action, documentWithTimestamp, version, operation.getIndex(), null);
        return operation.getEvent() != null
                ? new BulkOperationWrapper(bulkOperationWithTimestamp, operation.getEvent(), null, operation.getTermValue())
                : new BulkOperationWrapper(bulkOperationWithTimestamp, operation.getEventHandle(), null, operation.getTermValue());
    }

    private SerializedJson addTimestamp(final SerializedJson document, final String timestampField, final EventHandle eventHandle) {
        final JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(document.getSerializedJson());
        } catch (final IOException e) {
            return null;
        }
        if (!(jsonNode instanceof ObjectNode) || jsonNode.hasNonNull(timestampField)) {
            return null;
        }
        final ObjectNode json = (ObjectNode) jsonNode;
        if (json.hasNonNull(DEFAULT_TIMESTAMP_FIELD)) {
            json.set(timestampField, json.remove(DEFAULT_TIMESTAMP_FIELD));
        } else {
            final Instant originationTime = eventHandle != null ? eventHandle.getInternalOriginationTime() : null;
            json.put(timestampField, (originationTime != null ? originationTime : Instant.now()).toEpochMilli());
        }
        return SerializedJson.builder()
                .withJsonString(json.toString())
                .withDocumentId(document.getDocumentId().orElse(null))
                .withRoutingField(document.getRoutingField().orElse(null))
                .withPipelineField(document.getPipelineField().orElse(null))
                .withResolvedScriptParameters(document.getResolvedScriptParameters().orElse(null))
                .build();
    }
}
