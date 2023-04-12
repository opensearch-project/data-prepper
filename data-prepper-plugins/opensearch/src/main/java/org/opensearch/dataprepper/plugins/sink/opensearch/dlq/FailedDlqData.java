package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

import java.util.Objects;
import org.opensearch.dataprepper.model.event.EventHandle;

public class FailedDlqData {

    private final String index;
    private final String indexId;
    private final int status;
    private final String message;
    private final Object document;
    private final EventHandle eventHandle;

    private FailedDlqData(final String index, final String indexId, final int status, final String message, final Object document, final EventHandle eventHandle) {
        Objects.requireNonNull(index);
        this.index = index;
        this.indexId = indexId;
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        Objects.requireNonNull(document);
        this.document = document;
        this.eventHandle = eventHandle;
    }

    public String getIndex() {
        return index;
    }

    public int getStatus() {
        return status;
    }

    public String getIndexId() {
        return indexId;
    }

    public String getMessage() {
        return message;
    }

    public Object getDocument() {
        return document;
    }

    public EventHandle getEventHandle() {
        return eventHandle;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FailedDlqData that = (FailedDlqData) o;
        return Objects.equals(index, that.index) &&
            Objects.equals(indexId, that.indexId) &&
            Objects.equals(status, that.status) &&
            Objects.equals(message, that.message) &&
            Objects.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, indexId, status, message, document);
    }

    @Override
    public String toString() {
        return "FailedDlqData{" +
            "index='" + index + '\'' +
            ", indexId='" + indexId + '\'' +
            ", status='" + status + '\'' +
            ", message='" + message + '\'' +
            ", document=" + document +
            '}';
    }

    public static FailedDlqData.Builder builder() {
        return new FailedDlqData.Builder();
    }

    public static class Builder {

        private String index;
        private String indexId;
        private EventHandle eventHandle;

        private int status = 0;

        private String message;

        private Object document;

        public Builder withIndex(final String index) {
            this.index = index;
            return this;
        }

        public Builder withIndexId(final String indexId) {
            this.indexId = indexId;
            return this;
        }

        public Builder withStatus(final int status) {
            this.status = status;
            return this;
        }

        public Builder withMessage(final String message) {
            this.message = message;
            return this;
        }

        public Builder withDocument(final Object document) {
            this.document = document;
            return this;
        }

        public Builder withEventHandle(final EventHandle eventHandle) {
            this.eventHandle = eventHandle;
            return this;
        }

        public FailedDlqData build() {
            return new FailedDlqData(index, indexId, status, message, document, eventHandle);
        }
    }
}
