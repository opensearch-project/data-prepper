/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JavaClientAccumulatingBulkRequest implements AccumulatingBulkRequest<BulkOperation, BulkRequest> {
    static final int OPERATION_OVERHEAD = 50;

    private final List<BulkOperation> bulkOperations;
    private BulkRequest.Builder bulkRequestBuilder;
    private long currentBulkSize = 0L;
    private int operationCount = 0;
    private int retryCount = 0;
    private BulkRequest builtRequest;

    public JavaClientAccumulatingBulkRequest(BulkRequest.Builder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
    }

    @Override
    public long estimateSizeInBytesWithDocument(BulkOperation documentOrOperation) {
        return currentBulkSize + estimateBulkOperationSize(documentOrOperation);
    }

    @Override
    public void addOperation(BulkOperation bulkOperation) {
        final Long documentLength = estimateBulkOperationSize(bulkOperation);

        currentBulkSize += documentLength;

        bulkRequestBuilder = bulkRequestBuilder.operations(bulkOperation);

        operationCount++;
        bulkOperations.add(bulkOperation);
    }

    @Override
    public BulkOperation getOperationAt(int index) {
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
    public int incrementAndGetRetryCount() {
        return ++retryCount;
    }

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public void setRetryCount(int count) {
        retryCount = count;
    }

    @Override
    public List<BulkOperation> getOperations() {
        return Collections.unmodifiableList(bulkOperations);
    }

    @Override
    public BulkRequest getRequest() {
        if(builtRequest == null)
            builtRequest = bulkRequestBuilder.build();
        return builtRequest;
    }

    private long estimateBulkOperationSize(BulkOperation bulkOperation) {

        Object anyDocument;

        if (bulkOperation.isIndex()) {
            anyDocument = bulkOperation.index().document();
        } else if (bulkOperation.isCreate()) {
            anyDocument = bulkOperation.create().document();
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
