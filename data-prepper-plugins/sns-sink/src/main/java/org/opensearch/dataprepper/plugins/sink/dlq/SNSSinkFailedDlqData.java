/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

public class SNSSinkFailedDlqData {

    private String topic;

    private String errorMsg;

    private String bufferData;

    public SNSSinkFailedDlqData(String topic, String errorMsg, String bufferData) {
        this.topic = topic;
        this.errorMsg = errorMsg;
        this.bufferData = bufferData;
    }

    public String getTopic() {
        return topic;
    }

    public SNSSinkFailedDlqData setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public SNSSinkFailedDlqData setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        return this;
    }

    public String getBufferData() {
        return bufferData;
    }

    public SNSSinkFailedDlqData setBufferData(String bufferData) {
        this.bufferData = bufferData;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "topic='" + topic + '\'' +
                ", errorMsg='" + errorMsg + '\'' +
                ", bufferData='" + bufferData + '\'' +
                '}';
    }
}
