/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus;

public class HttpEndPointResponse {
    private String url;
    private int statusCode;
    private String errorMessage;

    public HttpEndPointResponse(final String url,
                                final int statusCode,
                                final String errorMessage) {
        this.url = url;
        this.statusCode = statusCode;
        this.errorMessage = errorMessage;
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

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "{" +
                "url='" + url + '\'' +
                ", statusCode=" + statusCode +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}