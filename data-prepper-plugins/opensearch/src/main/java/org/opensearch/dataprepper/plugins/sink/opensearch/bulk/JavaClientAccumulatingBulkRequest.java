/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class JavaClientAccumulatingBulkRequest implements AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(JavaClientAccumulatingBulkRequest.class);

    private final List<BulkOperationWrapper> bulkOperations;
    private BulkRequest.Builder bulkRequestBuilder;
    private long currentBulkSize = 0L;
    private long sampledOperationSize = 0L;
    private int operationCount = 0;
    private BulkRequest builtRequest;

    public JavaClientAccumulatingBulkRequest(BulkRequest.Builder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
    }

    @Override
    public long estimateSizeInBytesWithDocument(BulkOperationWrapper documentOrOperation) {
        return sampledOperationSize + currentBulkSize;
    }

    @Override
    public void addOperation(BulkOperationWrapper bulkOperation) {
        if (bulkOperations.size() == 5000) {
            currentBulkSize = estimateBulkSize();
            sampledOperationSize = currentBulkSize / 5000;
        }

        currentBulkSize = sampledOperationSize * bulkOperations.size();

        bulkRequestBuilder = bulkRequestBuilder.operations(bulkOperation.getBulkOperation());

        operationCount++;
        bulkOperations.add(bulkOperation);
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
        try {
            final List<Object> documents = bulkOperations.stream()
                    .map(BulkOperationWrapper::getBulkOperation)
                    .map(BulkOperation::index)
                    .map(IndexOperation::document)
                    .collect(Collectors.toList());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
            ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut);
            objectOut.writeObject(documents);
            objectOut.close();
            final long compDocLength = baos.toByteArray().length;
            LOG.warn("Compressed length: {}", compDocLength);

            return compDocLength;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
