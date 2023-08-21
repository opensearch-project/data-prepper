/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.handler;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;


public class HttpAuthOptions {

    private String url;

    private HttpClientBuilder httpClientBuilder;

    private ClassicRequestBuilder classicHttpRequestBuilder;

    private HttpClientConnectionManager httpClientConnectionManager;

    private int workers;

    private HttpHost proxy;

    public HttpClientBuilder getHttpClientBuilder() {
        return httpClientBuilder;
    }

    public ClassicRequestBuilder getClassicHttpRequestBuilder() {
        return classicHttpRequestBuilder;
    }

    public int getWorkers() {
        return workers;
    }

    public String getUrl() {
        return url;
    }

    public HttpHost getProxy() {
        return proxy;
    }

    public HttpClientConnectionManager getHttpClientConnectionManager() {
        return httpClientConnectionManager;
    }

    private HttpAuthOptions(Builder builder) {
        this.url = builder.url;
        this.httpClientBuilder = builder.httpClientBuilder;
        this.classicHttpRequestBuilder = builder.classicHttpRequestBuilder;
        this.httpClientConnectionManager = builder.httpClientConnectionManager;
        this.workers = builder.workers;
        this.proxy = builder.proxy;
    }
    public static class Builder {

        private String url;
        private HttpClientBuilder httpClientBuilder;
        private ClassicRequestBuilder classicHttpRequestBuilder;
        private HttpClientConnectionManager httpClientConnectionManager;
        private int workers;
        private HttpHost proxy;

        public HttpAuthOptions build() {
            return new HttpAuthOptions(this);
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public String getUrl() {
            return url;
        }

        public Builder setHttpClientBuilder(HttpClientBuilder httpClientBuilder) {
            this.httpClientBuilder = httpClientBuilder;
            return this;
        }

        public Builder setClassicHttpRequestBuilder(ClassicRequestBuilder classicHttpRequestBuilder) {
            this.classicHttpRequestBuilder = classicHttpRequestBuilder;
            return this;
        }

        public Builder setHttpClientConnectionManager(HttpClientConnectionManager httpClientConnectionManager) {
            this.httpClientConnectionManager = httpClientConnectionManager;
            return this;
        }

        public Builder setWorkers(int workers) {
            this.workers = workers;
            return this;
        }

        public Builder setProxy(HttpHost proxy) {
            this.proxy = proxy;
            return this;
        }

        public ClassicRequestBuilder getClassicHttpRequestBuilder() {
            return classicHttpRequestBuilder;
        }
    }

}
