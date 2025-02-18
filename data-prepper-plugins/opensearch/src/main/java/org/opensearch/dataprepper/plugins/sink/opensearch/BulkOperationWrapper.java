/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class BulkOperationWrapper {
    private static final Predicate<BulkOperation> IS_INDEX_OPERATION = BulkOperation::isIndex;
    private static final Predicate<BulkOperation> IS_CREATE_OPERATION = BulkOperation::isCreate;
    private static final Predicate<BulkOperation> IS_UPDATE_OPERATION = BulkOperation::isUpdate;
    private static final Predicate<BulkOperation> IS_DELETE_OPERATION = BulkOperation::isDelete;

    private static final Map<Predicate<BulkOperation>, Function<BulkOperation, Object>> BULK_OPERATION_TO_DOCUMENT_CONVERTERS = Map.of(
            IS_INDEX_OPERATION, operation -> operation.index().document(),
            IS_CREATE_OPERATION, operation -> operation.create().document()
    );

    private static final Map<Predicate<BulkOperation>, Function<BulkOperation, String>> BULK_OPERATION_TO_INDEX_NAME_CONVERTERS = Map.of(
            IS_INDEX_OPERATION, operation -> operation.index().index(),
            IS_CREATE_OPERATION, operation -> operation.create().index(),
            IS_UPDATE_OPERATION, operation -> operation.update().index(),
            IS_DELETE_OPERATION, operation -> operation.delete().index()
    );

    private static final Map<Predicate<BulkOperation>, Function<BulkOperation, String>> BULK_OPERATION_TO_ID_CONVERTERS = Map.of(
            IS_INDEX_OPERATION, operation -> operation.index().id(),
            IS_CREATE_OPERATION, operation -> operation.create().id(),
            IS_UPDATE_OPERATION, operation -> operation.update().id(),
            IS_DELETE_OPERATION, operation -> operation.delete().id()
    );

    private final EventHandle eventHandle;
    private final BulkOperation bulkOperation;
    private final SerializedJson jsonNode;

    private final String queryTerm;

    public BulkOperationWrapper(final BulkOperation bulkOperation) {
        this(bulkOperation, null, null, null);
    }

    public BulkOperationWrapper(final BulkOperation bulkOperation, final EventHandle eventHandle, final SerializedJson jsonNode,
                                final String queryTerm) {
        checkNotNull(bulkOperation);
        this.bulkOperation = bulkOperation;
        this.eventHandle = eventHandle;
        this.jsonNode = jsonNode;
        this.queryTerm = queryTerm;
    }

    public BulkOperationWrapper(final BulkOperation bulkOperation, final EventHandle eventHandle) {
        this(bulkOperation, eventHandle, null, null);
    }

    public BulkOperation getBulkOperation() {
        return bulkOperation;
    }

    public EventHandle getEventHandle() {
        return eventHandle;
    }

    public void releaseEventHandle(boolean result) {
        eventHandle.release(result);
    }

    public Object getDocument() {
        if (bulkOperation.isUpdate() || bulkOperation.isDelete()) {
            return jsonNode;
        }
        return getValueFromConverter(BULK_OPERATION_TO_DOCUMENT_CONVERTERS);
    }

    public String getTermValue() {
        return queryTerm;
    }

    public String getIndex() {
        return getValueFromConverter(BULK_OPERATION_TO_INDEX_NAME_CONVERTERS);
    }

    public String getId() {
        return getValueFromConverter(BULK_OPERATION_TO_ID_CONVERTERS);
    }

    private <T> T getValueFromConverter(final Map<Predicate<BulkOperation>, Function<BulkOperation, T>> converters) {
        final List<T> values = converters.entrySet().stream()
                .filter(entry -> entry.getKey().test(bulkOperation))
                .map(entry -> entry.getValue().apply(bulkOperation))
                .collect(Collectors.toList());

        if (values.size() != 1) {
            throw new UnsupportedOperationException("Only index or create operations are supported currently." + bulkOperation);
        }

        return values.get(0);
    }
}
