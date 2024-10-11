/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink.dlq;

import software.amazon.awssdk.core.SdkBytes;


public class LambdaSinkFailedDlqData {

    private SdkBytes payload;

    private String message;

    private int status;

    public LambdaSinkFailedDlqData(SdkBytes payload, String message, int status)  {
        this.payload = payload;
        this.message = message;
        this.status = status;
    }

    public SdkBytes getPayload() {
        return payload;
    }

    public LambdaSinkFailedDlqData setPayload(SdkBytes payload) {
        this.payload = payload;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public LambdaSinkFailedDlqData setMessage(String message) {
        this.message = message;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public LambdaSinkFailedDlqData setStatus(int status) {
        this.status = status;
        return this;
    }

    @Override
    public String toString() {

        return "failedData\n" +
                "payload    \"" + payload.asUtf8String() + "\"\n" +
                "message    \"" + message + "\"\n" +
                "status    \"" + status + "\n";
    }
}
