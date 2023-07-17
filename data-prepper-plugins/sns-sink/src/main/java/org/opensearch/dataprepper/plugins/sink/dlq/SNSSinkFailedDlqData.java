/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SNSSinkFailedDlqData {

    private String topic;

    private String errorMsg;

    @JsonIgnore
    private String bufferData;

    private String timeStamp;

    public SNSSinkFailedDlqData(final Builder builder) {
        this.bufferData = builder.bufferData;
    }

    public String getBufferData() {
        return bufferData;
    }

    public String getTopic() {
        return topic;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    @Override
    public String toString() {
        return "{" +
                "topic='" + topic + '\'' +
                ", errorMsg='" + errorMsg + '\'' +
                ", bufferData='" + bufferData + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String topic;

        private String errorMsg;

        @JsonIgnore
        private String bufferData;

        private String timeStamp;

        public Builder setTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
            return this;
        }

        public Builder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withErrorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
            return this;
        }

        public Builder withBufferData(String bufferData) {
            this.bufferData = bufferData;
            return this;
        }

        public SNSSinkFailedDlqData build() {
            return new SNSSinkFailedDlqData(this);
        }
    }
}
