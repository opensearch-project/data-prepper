/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http;

public class HttpEndpointResponse {
    private String url;
    private int statusCode;
    private String errMessage;

    public HttpEndpointResponse(final String url,
                                final int statusCode,
                                final String errMessage) {
        this.url = url;
        this.statusCode = statusCode;
        this.errMessage = errMessage;
    }

    public HttpEndpointResponse(final String url,
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
