/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWithHandle;
import org.opensearch.client.opensearch.core.BulkRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JavaClientAccumulatingBulkRequest implements AccumulatingBulkRequest<BulkOperationWithHandle, BulkRequest> {
    static final int OPERATION_OVERHEAD = 50;

    private final List<BulkOperationWithHandle> bulkOperations;
    private BulkRequest.Builder bulkRequestBuilder;
    private long currentBulkSize = 0L;
    private int operationCount = 0;
    private BulkRequest builtRequest;

    public JavaClientAccumulatingBulkRequest(BulkRequest.Builder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
    }

    @Override
    public long estimateSizeInBytesWithDocument(BulkOperationWithHandle documentOrOperation) {
        return currentBulkSize + estimateBulkOperationSize(documentOrOperation);
    }

    @Override
    public void addOperation(BulkOperationWithHandle bulkOperation) {
        final Long documentLength = estimateBulkOperationSize(bulkOperation);

        currentBulkSize += documentLength;

        bulkRequestBuilder = bulkRequestBuilder.operations(bulkOperation.getBulkOperation());

        operationCount++;
        bulkOperations.add(bulkOperation);
    }

    @Override
    public BulkOperationWithHandle getOperationAt(int index) {
        return bulkOperations.get(index);
    }

    @Override
    public long getEstimatedSizeInBytes() {
        return currentBulkSize;
    }

    @Override
    public int getOperationsCount() {
        return operationCount;
    }

    @Override
    public List<BulkOperationWithHandle> getOperations() {
        return Collections.unmodifiableList(bulkOperations);
    }

    @Override
    public BulkRequest getRequest() {
        if(builtRequest == null)
            builtRequest = bulkRequestBuilder.build();
        return builtRequest;
    }

    private long estimateBulkOperationSize(BulkOperationWithHandle bulkOperation) {

        Object anyDocument;

        if (bulkOperation.getBulkOperation().isIndex()) {
            anyDocument = bulkOperation.getBulkOperation().index().document();
        } else if (bulkOperation.getBulkOperation().isCreate()) {
            anyDocument = bulkOperation.getBulkOperation().create().document();
        } else {
            throw new UnsupportedOperationException("Only index or create operations are supported currently. " + bulkOperation);
        }

        if (anyDocument == null)
            return OPERATION_OVERHEAD;

        if (!(anyDocument instanceof SizedDocument)) {
            throw new IllegalArgumentException("Only SizedDocument is permitted for accumulating bulk requests. " + bulkOperation);
        }

        SizedDocument sizedDocument = (SizedDocument) anyDocument;

        final long documentLength = sizedDocument.getDocumentSize();
        return documentLength + OPERATION_OVERHEAD;

    }
}
