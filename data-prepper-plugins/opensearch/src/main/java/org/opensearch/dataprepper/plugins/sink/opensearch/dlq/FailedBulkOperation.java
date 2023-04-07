package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

import java.util.Objects;

public class FailedBulkOperation {

    private final BulkOperationWrapper bulkOperation;
    private final BulkResponseItem bulkResponseItem;
    private final Throwable failure;

    private FailedBulkOperation(final BulkOperationWrapper bulkOperation, final BulkResponseItem bulkResponseItem, final Throwable failure) {
        Objects.requireNonNull(bulkOperation);
        this.bulkOperation = bulkOperation;
        if (bulkResponseItem == null && failure == null) {
            throw new IllegalArgumentException("bulkResponseItem and failure cannot both be null. One must be provided.");
        }
        this.bulkResponseItem = bulkResponseItem;
        this.failure = failure;
    }

    public BulkOperationWrapper getBulkOperation() {
        return bulkOperation;
    }

    public BulkResponseItem getBulkResponseItem() {
        return bulkResponseItem;
    }

    public Throwable getFailure() {
        return failure;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FailedBulkOperation that = (FailedBulkOperation) o;
        return Objects.equals(bulkOperation, that.bulkOperation) &&
            Objects.equals(bulkResponseItem, that.bulkResponseItem) &&
            Objects.equals(failure, that.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkOperation, bulkResponseItem, failure);
    }

    @Override
    public String toString() {
        return "FailedBulkOperation{" +
            "bulkOperation='" + bulkOperation + '\'' +
            ", bulkResponseItem='" + bulkResponseItem + '\'' +
            ", failure=" + failure +
            '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private BulkOperationWrapper bulkOperation;
        private BulkResponseItem bulkResponseItem;
        private Throwable failure;

        public Builder withBulkOperation(final BulkOperationWrapper bulkOperation) {
            this.bulkOperation = bulkOperation;
            return this;
        }

        public Builder withBulkResponseItem(final BulkResponseItem bulkResponseItem) {
            this.bulkResponseItem = bulkResponseItem;
            return this;
        }

        public Builder withFailure(final Throwable failure) {
            this.failure = failure;
            return this;
        }

        public FailedBulkOperation build() {
            return new FailedBulkOperation(bulkOperation, bulkResponseItem, failure);
        }
    }
}
