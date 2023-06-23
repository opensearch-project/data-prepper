/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.client.opensearch.core.BulkRequest;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class JavaClientAccumulatingBulkRequest implements AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> {
    private final List<BulkOperationWrapper> bulkOperations;
    private final int sampleSize;
    private BulkRequest.Builder bulkRequestBuilder;
    private long currentBulkSize = 0L;
    private long sampledOperationSize = 0L;
    private int operationCount = 0;
    private BulkRequest builtRequest;

    public JavaClientAccumulatingBulkRequest(BulkRequest.Builder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
        this.sampleSize = 5000;
    }

    @VisibleForTesting
    JavaClientAccumulatingBulkRequest(BulkRequest.Builder bulkRequestBuilder, final int sampleSize) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
        this.sampleSize = sampleSize;
    }

    @Override
    public long estimateSizeInBytesWithDocument(BulkOperationWrapper documentOrOperation) {
        return currentBulkSize + sampledOperationSize;
    }

    @Override
    public void addOperation(BulkOperationWrapper bulkOperation) {
        bulkRequestBuilder = bulkRequestBuilder.operations(bulkOperation.getBulkOperation());

        operationCount++;
        bulkOperations.add(bulkOperation);

        if (bulkOperations.size() == sampleSize) {
            currentBulkSize = estimateBulkSize();
            sampledOperationSize = currentBulkSize / sampleSize;
        } else {
            currentBulkSize += sampledOperationSize;
        }
    }

    @Override
    public BulkOperationWrapper getOperationAt(int index) {
        return bulkOperations.get(index);
    }

    @Override
    public long getEstimatedSizeInBytes() {
        if (currentBulkSize == 0) {
            currentBulkSize = estimateBulkSize();
        }

        return currentBulkSize;
    }

    @Override
    public int getOperationsCount() {
        return operationCount;
    }

    @Override
    public List<BulkOperationWrapper> getOperations() {
        return Collections.unmodifiableList(bulkOperations);
    }

    @Override
    public BulkRequest getRequest() {
        if(builtRequest == null)
            builtRequest = bulkRequestBuilder.build();
        return builtRequest;
    }

    private long estimateBulkSize() {
        final List<Object> documents = bulkOperations.stream()
                .map(this::mapBulkOperationToDocument)
                .collect(Collectors.toList());

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
            final ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut);
            objectOut.writeObject(documents);
            objectOut.close();

            return baos.toByteArray().length;
        } catch (final Exception e) {
            throw new RuntimeException("Caught exception measuring compressed bulk request size.", e);
        }
    }

    private Object mapBulkOperationToDocument(final BulkOperationWrapper bulkOperation) {
        Object anyDocument;

        if (bulkOperation.getBulkOperation().isIndex()) {
            anyDocument = bulkOperation.getBulkOperation().index().document();
        } else if (bulkOperation.getBulkOperation().isCreate()) {
            anyDocument = bulkOperation.getBulkOperation().create().document();
        } else {
            throw new UnsupportedOperationException("Only index or create operations are supported currently. " + bulkOperation);
        }

        if (anyDocument == null) {
            return new SerializedJsonImpl(null);
        }

        if (!(anyDocument instanceof Serializable)) {
            throw new IllegalArgumentException("Only classes implementing Serializable are permitted for accumulating bulk requests. " + bulkOperation);
        }

        return anyDocument;
    }
}
