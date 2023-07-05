/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class JavaClientAccumulatingCompressedBulkRequest implements AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(JavaClientAccumulatingCompressedBulkRequest.class);

    private final List<BulkOperationWrapper> bulkOperations;
    private long sampleSize;
    private final long targetBulkSize;
    private final int maxLocalCompressionsForEstimation;
    private BulkRequest.Builder bulkRequestBuilder;
    private long currentBulkSize = 0L;
    private double sampledOperationSize = 0.0;
    private int operationCount = 0;
    private int timesSampled = 0;
    private BulkRequest builtRequest;

    public JavaClientAccumulatingCompressedBulkRequest(final BulkRequest.Builder bulkRequestBuilder, final long targetBulkSize,
                                                       final int maxLocalCompressionsForEstimation) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
        this.targetBulkSize = targetBulkSize;
        this.maxLocalCompressionsForEstimation = maxLocalCompressionsForEstimation;
        // Set the sample size to 10 to get an initial data point
        this.sampleSize = 10;
    }

    @VisibleForTesting
    JavaClientAccumulatingCompressedBulkRequest(final BulkRequest.Builder bulkRequestBuilder, final long targetBulkSize,
                                                final int maxLocalCompressionsForEstimation, final int sampleSize) {
        this.bulkRequestBuilder = bulkRequestBuilder;
        bulkOperations = new ArrayList<>();
        this.targetBulkSize = targetBulkSize;
        this.maxLocalCompressionsForEstimation = maxLocalCompressionsForEstimation;
        this.sampleSize = sampleSize;
    }

    @Override
    public long estimateSizeInBytesWithDocument(BulkOperationWrapper documentOrOperation) {
        return currentBulkSize + (long) sampledOperationSize;
    }

    @Override
    public void addOperation(BulkOperationWrapper bulkOperation) {
        bulkRequestBuilder = bulkRequestBuilder.operations(bulkOperation.getBulkOperation());

        operationCount++;
        bulkOperations.add(bulkOperation);

        if (timesSampled < maxLocalCompressionsForEstimation && bulkOperations.size() == sampleSize) {
            currentBulkSize = estimateBulkSize();
            sampledOperationSize = (double) currentBulkSize / (double) bulkOperations.size();
            updateTargetSampleSize();
            timesSampled++;
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

    private void updateTargetSampleSize() {
        final long remainingBytes = targetBulkSize - currentBulkSize;
        final double remainingBytesAsPercentage = ((double) remainingBytes / (double) targetBulkSize) * 100d;

        if (remainingBytesAsPercentage < 10d) {
            LOG.debug("Found remaining percentage of {}, current size {}, target size {}. Skipping further estimations",
                    remainingBytesAsPercentage, currentBulkSize, targetBulkSize);
            // If we have packed at least 90% of the bulk request already, assume the sampled operation size is sufficient
            // and continue with that estimate rather than eating the overhead of more local compressions
            return;
        }

        final double estimatedRemainingOperationsUntilFull = (double) remainingBytes / sampledOperationSize;

        if (estimatedRemainingOperationsUntilFull < 100d) {
            LOG.debug("Found estimated remaining operations of {}. Skipping further estimations", estimatedRemainingOperationsUntilFull);
            // If we have less than 100 estimated operations until the bulk request is full, assume the sampled operation size is sufficient
            // and continue with that estimate rather than eating the overhead of more local compressions
            return;
        }

        final double operationsUntilNextSample = estimatedRemainingOperationsUntilFull / 2d;
        LOG.debug("{} bytes remaining to {}. Average size so far {}, checking again in {} ops", targetBulkSize - currentBulkSize, targetBulkSize,
                sampledOperationSize, operationsUntilNextSample);

        sampleSize += operationsUntilNextSample;
    }
}
