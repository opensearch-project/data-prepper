/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

public class SNSSinkFailedDlqData {

    private String topic;

    private String message;

    private int status;

    public SNSSinkFailedDlqData(String topic, String message, int status) {
        this.topic = topic;
        this.message = message;
        this.status = status;
    }

    public String getTopic() {
        return topic;
    }

    public SNSSinkFailedDlqData setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public SNSSinkFailedDlqData setMessage(String message) {
        this.message = message;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public SNSSinkFailedDlqData setStatus(int status) {
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
