/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;

import java.util.List;

public class FailedDlqData {

    private final List<HttpEndPointResponse> endPointResponses;
    @JsonIgnore
    private final String bufferData;

    public FailedDlqData(final Builder builder) {
        this.endPointResponses = builder.endPointResponses;
        this.bufferData = builder.bufferData;
    }

    public List<HttpEndPointResponse> getEndPointResponses() {
        return endPointResponses;
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
                "endPointResponses=" + endPointResponses +
                ", bufferData='" + bufferData + '\'' +
                '}';
    }

    public static class Builder {

        private List<HttpEndPointResponse> endPointResponses;

        private String bufferData;

        public Builder withEndPointResponses(List<HttpEndPointResponse> endPointResponses) {
            this.endPointResponses = endPointResponses;
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
