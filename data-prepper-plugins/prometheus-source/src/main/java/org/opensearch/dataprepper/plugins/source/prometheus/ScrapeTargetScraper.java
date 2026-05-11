/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.HttpHeadersBuilder;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.HttpStatusClass;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.plugins.http.client.auth.HttpRequestAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wraps Armeria WebClient to scrape a single Prometheus /metrics endpoint.
 */
public class ScrapeTargetScraper {

    private static final Logger LOG = LoggerFactory.getLogger(ScrapeTargetScraper.class);
    private static final String ACCEPT_HEADER_VALUE = "text/plain;version=0.0.4";

    private final PrometheusScrapeConfig config;
    private final HttpHeaders commonHeaders;
    private final ClientFactory clientFactory;
    private final boolean isCustomFactory;
    private final HttpRequestAuthenticator authenticator;
    private final Map<String, WebClient> clientCache = new ConcurrentHashMap<>();

    public ScrapeTargetScraper(final PrometheusScrapeConfig config) {
        this.config = config;
        this.authenticator = HttpRequestAuthenticator.create(config.getAuthentication());
        this.commonHeaders = buildCommonHeaders();
        this.clientFactory = buildClientFactory();
        this.isCustomFactory = (clientFactory != ClientFactory.ofDefault());
    }

    /**
     * Scrapes the given Prometheus metrics endpoint URL.
     *
     * @param url the full URL of the metrics endpoint to scrape
     * @return the response body as a String
     * @throws RuntimeException if the response status is not 2xx
     */
    public String scrape(final String url) {
        final URI uri = URI.create(url);
        final String baseUri;
        try {
            baseUri = new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString();
        } catch (final URISyntaxException e) {
            throw new RuntimeException("Invalid scrape target URL: " + url, e);
        }
        final String path = uri.getRawPath() != null && !uri.getRawPath().isEmpty()
                ? uri.getRawPath() : "/";
        final String pathWithQuery = uri.getRawQuery() != null
                ? path + "?" + uri.getRawQuery() : path;

        final WebClient client = clientCache.computeIfAbsent(baseUri, key ->
                WebClient.builder(key)
                        .responseTimeoutMillis(config.getScrapeTimeout().toMillis())
                        .factory(clientFactory)
                        .build());

        final RequestHeadersBuilder requestHeadersBuilder = RequestHeaders.builder(HttpMethod.GET, pathWithQuery);
        requestHeadersBuilder.add(commonHeaders);
        final RequestHeaders requestHeaders = requestHeadersBuilder.build();

        final AggregatedHttpResponse response = client.execute(requestHeaders).aggregate().join();

        final HttpStatus status = response.status();
        if (status.codeClass() != HttpStatusClass.SUCCESS) {
            throw new RuntimeException(String.format(
                    "Failed to scrape %s: received HTTP %d", url, status.code()));
        }

        return response.contentUtf8();
    }

    /**
     * Closes the underlying ClientFactory if it is not the default factory.
     */
    public void close() {
        if (isCustomFactory) {
            clientFactory.close();
        }
    }

    private HttpHeaders buildCommonHeaders() {
        final HttpHeadersBuilder builder = HttpHeaders.builder()
                .add(HttpHeaderNames.ACCEPT, ACCEPT_HEADER_VALUE);

        if (authenticator != null) {
            authenticator.applyAuth(builder);
        }

        return builder.build();
    }

    private ClientFactory buildClientFactory() {
        if (config.getSslCertificateFile() != null || config.isInsecure()) {
            final ClientFactoryBuilder factoryBuilder = ClientFactory.builder();
            if (config.isInsecure()) {
                factoryBuilder.tlsNoVerify();
            } else if (config.getSslCertificateFile() != null) {
                factoryBuilder.tlsCustomizer(sslCtxBuilder ->
                        sslCtxBuilder.trustManager(new File(config.getSslCertificateFile())));
            }
            return factoryBuilder.build();
        }
        return ClientFactory.ofDefault();
    }
}
