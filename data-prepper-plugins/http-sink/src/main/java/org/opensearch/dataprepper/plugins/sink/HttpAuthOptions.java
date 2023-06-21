/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;

public class HttpAuthOptions {
    private String url;

    private CloseableHttpClient closeableHttpClient;

    private ClassicHttpRequest classicHttpRequest;

    private int workers;

    private String proxy;

    public CloseableHttpClient getCloseableHttpClient() {
        return closeableHttpClient;
    }

    public HttpAuthOptions setCloseableHttpClient(CloseableHttpClient closeableHttpClient) {
        this.closeableHttpClient = closeableHttpClient;
        return this;
    }

    public ClassicHttpRequest getClassicHttpRequest() {
        return classicHttpRequest;
    }

    public HttpAuthOptions setClassicHttpRequest(ClassicHttpRequest classicHttpRequest) {
        this.classicHttpRequest = classicHttpRequest;
        return this;
    }

    public int getWorkers() {
        return workers;
    }

    public HttpAuthOptions setWorkers(int workers) {
        this.workers = workers;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public HttpAuthOptions setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getProxy() {
        return proxy;
    }

    public HttpAuthOptions setProxy(String proxy) {
        this.proxy = proxy;
        return this;
    }
}
