/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;

public class FailedDlqData {

    private final HttpEndPointResponse endPointResponse;
    @JsonIgnore
    private final String bufferData;

    public FailedDlqData(final Builder builder) {
        this.endPointResponse = builder.endPointResponse;
        this.bufferData = builder.bufferData;
    }

    public HttpEndPointResponse getEndPointResponse() {
        return endPointResponse;
    }

    public String getBufferData() {
        return bufferData;
    }

    public static Builder builder() {
        return new Builder();
    }
    @Override
    public String toString() {
        return "{" +
                "endPointResponse=" + endPointResponse +
                ", bufferData='" + bufferData + '\'' +
                '}';
    }

    public static class Builder {

        private HttpEndPointResponse endPointResponse;

        private String bufferData;

        public Builder withEndPointResponses(HttpEndPointResponse endPointResponses) {
            this.endPointResponse = endPointResponses;
            return this;
        }

        public Builder withBufferData(String bufferData) {
            this.bufferData = bufferData;
            return this;
        }

        public FailedDlqData build() {
            return new FailedDlqData(this);
        }
    }
}
