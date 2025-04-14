package org.opensearch.dataprepper.plugins.sink.opensearch.index.model;

import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;

import java.time.Instant;

public class QueryManagerBulkOperation {

    private final BulkOperationWrapper bulkOperationWrapper;

    private final Instant startTime;

    private final String termValue;

    public QueryManagerBulkOperation(final BulkOperationWrapper bulkOperationWrapper,
                                     final Instant startTime,
                                     final String termValue) {
        this.bulkOperationWrapper = bulkOperationWrapper;
        this.startTime = startTime;
        this.termValue = termValue;
    }

    public BulkOperationWrapper getBulkOperationWrapper() {
        return bulkOperationWrapper;
    }

    public Instant getStartTime() {
        return startTime;
    }

    // Write comparator on term value and bulk operation index
    public String getTermValue() {
        return termValue;
    }
}
