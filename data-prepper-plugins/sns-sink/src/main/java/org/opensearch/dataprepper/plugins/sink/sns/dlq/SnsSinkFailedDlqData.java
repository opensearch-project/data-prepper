/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns.dlq;

public class SnsSinkFailedDlqData {

    private String topic;

    private String message;

    private int status;

    public SnsSinkFailedDlqData(String topic, String message, int status) {
        this.topic = topic;
        this.message = message;
        this.status = status;
    }

    public String getTopic() {
        return topic;
    }

    public SnsSinkFailedDlqData setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public SnsSinkFailedDlqData setMessage(String message) {
        this.message = message;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public SnsSinkFailedDlqData setStatus(int status) {
        this.status = status;
        return this;
    }

    @Override
    public String toString() {
        return "failedData\n" +
                "topic    \"" + topic + "\"\n" +
                "message    \"" + message + "\"\n" +
                "status    \"" + status +"\n";
    }
}
