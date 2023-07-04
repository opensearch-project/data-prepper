/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

public class HttpEndPointResponse{
    private String url;
    private int statusCode;
    private String errMessage;

    public HttpEndPointResponse(final String url,
                                final int statusCode,
                                final String errMessage) {
        this.url = url;
        this.statusCode = statusCode;
        this.errMessage = errMessage;
    }

    public HttpEndPointResponse(final String url,
                                final int statusCode) {
        this.url = url;
        this.statusCode = statusCode;
    }

    public String getUrl() {
        return url;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getErrMessage() {
        return errMessage;
    }

    @Override
    public String toString() {
        return "{" +
                "url='" + url + '\'' +
                ", statusCode=" + statusCode +
                ", errMessage='" + errMessage + '\'' +
                '}';
    }
}