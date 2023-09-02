/*
 * Copyright OpenSearch Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

/**
 * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link Signer}
 * and {@link AwsCredentialsProvider}.
 */
final class AwsRequestSigningApacheInterceptor implements HttpRequestInterceptor {

    /**
     * Constant to check content-length
     */
    private static final String CONTENT_LENGTH = "content-length";
    /**
     * Constant to check Zero content length
     */
    private static final String ZERO_CONTENT_LENGTH = "0";
    /**
     * Constant to check if host is the endpoint
     */
    private static final String HOST = "host";

    /**
     * The service that we're connecting to.
     */
    private final String service;

    /**
     * The particular signer implementation.
     */
    private final Signer signer;

    /**
     * The source of AWS credentials for signing.
     */
    private final AwsCredentialsProvider awsCredentialsProvider;

    /**
     * The region signing region.
     */
    private final Region region;

    /**
     *
     * @param service service that we're connecting to
     * @param signer particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     * @param region signing region
     */
    public AwsRequestSigningApacheInterceptor(final String service,
                                              final Signer signer,
                                              final AwsCredentialsProvider awsCredentialsProvider,
                                              final Region region) {
        this.service = Objects.requireNonNull(service);
        this.signer =  Objects.requireNonNull(signer);
        this.awsCredentialsProvider =  Objects.requireNonNull(awsCredentialsProvider);
        this.region = Objects.requireNonNull(region);
    }

    /**
     *
     * @param service service that we're connecting to
     * @param signer particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     * @param region signing region
     */
    public AwsRequestSigningApacheInterceptor(final String service,
                                              final Signer signer,
                                              final AwsCredentialsProvider awsCredentialsProvider,
                                              final String region) {
        this(service, signer, awsCredentialsProvider, Region.of(region));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws IOException {
        URIBuilder uriBuilder;
        try {
            uriBuilder = new URIBuilder(request.getRequestLine().getUri());
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI" , e);
        }

        // Copy Apache HttpRequest to AWS Request
        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.fromValue(request.getRequestLine().getMethod()))
                .uri(buildUri(context, uriBuilder));

        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest =
                    (HttpEntityEnclosingRequest) request;
            if (httpEntityEnclosingRequest.getEntity() != null) {
                InputStream content = httpEntityEnclosingRequest.getEntity().getContent();
                requestBuilder.contentStreamProvider(() -> content);
            }
        }
        requestBuilder.rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()));
        requestBuilder.headers(headerArrayToMap(request.getAllHeaders()));

        ExecutionAttributes attributes = new ExecutionAttributes();
        attributes.putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS, awsCredentialsProvider.resolveCredentials());
        attributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, service);
        attributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, region);

        // Sign it
        SdkHttpFullRequest signedRequest = signer.sign(requestBuilder.build(), attributes);

        // Now copy everything back
        request.setHeaders(mapToHeaderArray(signedRequest.headers()));
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest =
                    (HttpEntityEnclosingRequest) request;
            if (httpEntityEnclosingRequest.getEntity() != null) {
                BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
                basicHttpEntity.setContent(signedRequest.contentStreamProvider()
                        .orElseThrow(() -> new IllegalStateException("There must be content"))
                        .newStream());
                httpEntityEnclosingRequest.setEntity(basicHttpEntity);
            }
        }
    }

    private URI buildUri(final HttpContext context, URIBuilder uriBuilder) throws IOException {
        try {
            HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);

            if (host != null) {
                uriBuilder.setHost(host.getHostName());
                uriBuilder.setScheme(host.getSchemeName());
                uriBuilder.setPort(host.getPort());
            }

            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI", e);
        }
    }

    /**
     *
     * @param params list of HTTP query params as NameValuePairs
     * @return a multimap of HTTP query params
     */
    private static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
        Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (NameValuePair nvp : params) {
            List<String> argsList =
                    parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
            argsList.add(nvp.getValue());
        }
        return parameterMap;
    }

    /**
     * @param headers modelled Header objects
     * @return a Map of header entries
     */
    private static Map<String, List<String>> headerArrayToMap(final Header[] headers) {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Header header : headers) {
            if (!skipHeader(header)) {
                headersMap.put(header.getName(), headersMap
                        .getOrDefault(header.getName(),
                                new LinkedList<>(Collections.singletonList(header.getValue()))));
            }
        }
        return headersMap;
    }

    /**
     * @param header header line to check
     * @return true if the given header should be excluded when signing
     */
    private static boolean skipHeader(final Header header) {
        return (CONTENT_LENGTH.equalsIgnoreCase(header.getName())
                && ZERO_CONTENT_LENGTH.equals(header.getValue())) // Strip Content-Length: 0
                || HOST.equalsIgnoreCase(header.getName()); // Host comes from endpoint
    }

    /**
     * @param mapHeaders Map of header entries
     * @return modelled Header objects
     */
    private static Header[] mapToHeaderArray(final Map<String, List<String>> mapHeaders) {
        Header[] headers = new Header[mapHeaders.size()];
        int i = 0;
        for (Map.Entry<String, List<String>> headerEntry : mapHeaders.entrySet()) {
            for (String value : headerEntry.getValue()) {
                headers[i++] = new BasicHeader(headerEntry.getKey(), value);
            }
        }
        return headers;
    }
}
