/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;

class HttpHeaderExtractorTest {

    @ParameterizedTest
    @CsvSource({
            "AUTHORIZATION, true",
            "proxy-authorization, true",
            "X-AMZ-SECURITY-TOKEN, true",
            "cookie, true",
            "set-cookie, true",
            "x-api-key, true",
            "x-csrf-token, true",
            "x-auth-token, true",
            "X-Tenant-Id, false",
            "Content-Type, false",
            "X-Request-Id, false"
    })
    void testIsSensitiveHeader(String headerName, boolean expected) {
        assertThat(HttpHeaderExtractor.isSensitiveHeader(headerName), equalTo(expected));
    }

    @Test
    void extractHeaders_returnsEmptyMap_whenMetadataHeadersIsEmpty() throws Exception {
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(Collections.emptyList());
        final AggregatedHttpRequest request = buildRequest(1, Map.of("X-Tenant-Id", "test"));

        assertThat(extractor.extractHeaders(request), anEmptyMap());
    }

    @Test
    void extractHeaders_extractsConfiguredHeaders() throws Exception {
        final String tenantId = UUID.randomUUID().toString();
        final String region = UUID.randomUUID().toString();
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id", "X-Region"));
        final AggregatedHttpRequest request = buildRequest(1,
                Map.of("X-Tenant-Id", tenantId, "X-Region", region));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.get("x-tenant-id"), equalTo(tenantId));
        assertThat(headers.get("x-region"), equalTo(region));
    }

    @Test
    void extractHeaders_ignoresNonConfiguredHeaders() throws Exception {
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id"));
        final AggregatedHttpRequest request = buildRequest(1,
                Map.of("X-Tenant-Id", "val1", "X-Other", "val2"));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.size(), equalTo(1));
        assertThat(headers.containsKey("x-other"), equalTo(false));
    }

    @Test
    void extractHeaders_ignoresMissingConfiguredHeaders() throws Exception {
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id", "X-Missing"));
        final AggregatedHttpRequest request = buildRequest(1, Map.of("X-Tenant-Id", "val1"));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.size(), equalTo(1));
        assertThat(headers.containsKey("x-missing"), equalTo(false));
    }

    @Test
    void extractHeaders_filtersSensitiveHeaders() throws Exception {
        final String tenantId = UUID.randomUUID().toString();
        final String authValue = UUID.randomUUID().toString();
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id", "authorization"));
        final AggregatedHttpRequest request = buildRequest(1,
                Map.of("X-Tenant-Id", tenantId, "authorization", authValue));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.get("x-tenant-id"), equalTo(tenantId));
        assertThat(headers.containsKey("authorization"), equalTo(false));
    }

    @Test
    void extractHeaders_normalizesHeaderKeysToLowercase() throws Exception {
        final String value = UUID.randomUUID().toString();
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id"));
        final AggregatedHttpRequest request = buildRequest(1, Map.of("X-Tenant-Id", value));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.containsKey("x-tenant-id"), equalTo(true));
        assertThat(headers.get("x-tenant-id"), equalTo(value));
    }

    @Test
    void extractHeaders_storesMultiValueHeaderAsList() throws Exception {
        final String ip1 = UUID.randomUUID().toString();
        final String ip2 = UUID.randomUUID().toString();
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Forwarded-For"));

        RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .add("X-Forwarded-For", ip1)
                .add("X-Forwarded-For", ip2);
        AggregatedHttpRequest request = HttpRequest.of(headersBuilder.build(),
                HttpData.ofUtf8("[{\"log\":\"test\"}]")).aggregate().get();

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.get("x-forwarded-for"), equalTo(List.of(ip1, ip2)));
    }

    @Test
    void extractHeaders_storesSingleValueHeaderAsString() throws Exception {
        final String value = UUID.randomUUID().toString();
        final HttpHeaderExtractor extractor = new HttpHeaderExtractor(List.of("X-Tenant-Id"));
        final AggregatedHttpRequest request = buildRequest(1, Map.of("X-Tenant-Id", value));

        final Map<String, Object> headers = extractor.extractHeaders(request);

        assertThat(headers.get("x-tenant-id") instanceof String, equalTo(true));
    }

    private AggregatedHttpRequest buildRequest(int numJson, Map<String, String> customHeaders)
            throws ExecutionException, InterruptedException {
        RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path("/log/ingest");
        for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
            headersBuilder.add(entry.getKey(), entry.getValue());
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < numJson; i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"log\":\"").append(UUID.randomUUID()).append("\"}");
        }
        sb.append("]");
        return HttpRequest.of(headersBuilder.build(), HttpData.ofUtf8(sb.toString())).aggregate().get();
    }
}
