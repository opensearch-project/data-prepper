/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.awaitility.Awaitility;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContent;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Provides utilities for integration testing against an OpenSearch cluster.
 * Much of this code comes from org.opensearch.test.rest.OpenSearchRestTestCase.
 * Using the OpenSearchRestTestCase class requires bringing in dependencies on OpenSearch Core
 * which are not relevant for Data Prepper.
 */
class OpenSearchIntegrationHelper {
    private static final String TRUSTSTORE_PATH = "truststore.path";
    private static final String TRUSTSTORE_PASSWORD = "truststore.password";
    private static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";
    private static final String CLIENT_PATH_PREFIX = "client.path.prefix";

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY =
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables());

    /**
     * Copied from OpenSearch test framework
     * Consider consolidating in OpenSearch
     */
    static List<String> getHosts() {
        return Arrays.stream(System.getProperty("tests.opensearch.host").split(","))
                .map(ip -> String.format("https://%s", ip)).collect(Collectors.toList());
    }

    static DeclaredOpenSearchVersion getVersion() {
        return DeclaredOpenSearchVersion.parse(System.getProperty("tests.opensearch.version"));
    }

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    static XContentParser createContentParser(XContent xContent, String data) throws IOException {
        return xContent.createParser(getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    private static NamedXContentRegistry getXContentRegistry() {
        return DEFAULT_NAMED_X_CONTENT_REGISTRY;
    }

    /**
     * Creates an OpenSearch low-level {@link RestClient}.
     * TODO: Consider using the RestHighLevelClient.
     * TODO: Consider consolidating with OpenSearch test framework.
     */
    static RestClient createOpenSearchClient() throws IOException {
        final List<String> hosts = getHosts();
        final HttpHost[] httpHosts = new HttpHost[hosts.size()];
        hosts.stream().map(HttpHost::create).collect(Collectors.toList()).toArray(httpHosts);
        final Settings settings = Settings.Builder.EMPTY_SETTINGS;
        return buildClient(settings, httpHosts);
    }

    /**
     * Created custom for Data Prepper
     */
    private static RestClient buildClient(final Settings settings, final HttpHost[] hosts) throws IOException {
        final RestClientBuilder builder = RestClient.builder(hosts);
        configureHttpsClient(builder, settings);

        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    /**
     * Determines if the target in OpenSearch. It appears to be true for OpenDistro as well.
     * Created custom for Data Prepper
     * TODO: Determine if we need this at all. Can we assume all supported versions of OpenDistro also have the same capabilities?
     * TODO: It may be possible to remove testing against HTTP after decoupling from the OpenSearch test framework Gradle plugin
     */
    static boolean isOSBundle() {
        final boolean osFlag = Optional.ofNullable(System.getProperty("tests.opensearch.bundle"))
                .map("true"::equalsIgnoreCase).orElse(false);
        if (osFlag) {
            // currently only external cluster is supported for security enabled testing
            if (!Optional.ofNullable(System.getProperty("tests.opensearch.host")).isPresent()) {
                throw new RuntimeException("cluster url should be provided for security enabled OpenSearch testing");
            }
        }

        return osFlag;
    }


    /**
     * Created custom for Data Prepper
     */
    private static void configureHttpsClient(final RestClientBuilder builder, final Settings settings) throws IOException {
        final Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        final Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            final String userName = Optional
                    .ofNullable(System.getProperty("tests.opensearch.user"))
                    .orElseThrow(() -> new RuntimeException("user name is missing"));
            final String password = Optional
                    .ofNullable(System.getProperty("tests.opensearch.password"))
                    .orElseThrow(() -> new RuntimeException("password is missing"));
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            try {
                return httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        // disable the certificate since our testing cluster just uses the default security configuration
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue
                .parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    static void wipeAllTemplates() throws IOException {
        final RestClient openSearchClient = createOpenSearchClient();
        openSearchClient.performRequest(new Request("DELETE", "_template/*"));
        try {
            openSearchClient.performRequest(new Request("DELETE", "_index_template/*"));
            openSearchClient.performRequest(new Request("DELETE", "_component_template/*"));
        } catch (ResponseException e) {
            // We hit a version of ES that doesn't support index templates v2 yet, so it's safe to ignore
        }
    }

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    static void waitForClusterStateUpdatesToFinish() throws Exception {
        final RestClient openSearchClient = createOpenSearchClient();

        Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> {
                    Response response = openSearchClient.performRequest(new Request("GET", "/_cluster/pending_tasks"));
                    List<?> tasks = (List<?>) entityAsMap(response).get("tasks");
                    return tasks.isEmpty();
                });
    }

    /**
     * Copied from OpenSearch test framework
     * TODO: Consolidate in OpenSearch
     */
    private static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (XContentParser parser = xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getEntity().getContent())) {
            return parser.map();
        }
    }
}
