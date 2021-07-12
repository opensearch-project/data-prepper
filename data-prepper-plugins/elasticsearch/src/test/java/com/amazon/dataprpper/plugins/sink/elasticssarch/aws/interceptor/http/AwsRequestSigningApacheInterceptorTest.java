/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.dataprpper.plugins.sink.elasticssarch.aws.interceptor.http;


import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;

public class AwsRequestSigningApacheInterceptorTest {
    private AwsRequestSigningApacheInterceptor interceptor;

    @Before
    void createInterceptor() {
        AwsCredentialsProvider anonymousCredentialsProvider =
                StaticCredentialsProvider.create(AnonymousCredentialsProvider.create().resolveCredentials());
        interceptor = new AwsRequestSigningApacheInterceptor("servicename",
                new AddHeaderSigner("Signature", "wuzzle"),
                anonymousCredentialsProvider,
                Region.AF_SOUTH_1);
    }

    @Test
    void testSimpleSigner() throws Exception {
        HttpEntityEnclosingRequest request =
                new BasicHttpEntityEnclosingRequest(new MockRequestLine("/query?a=b"));
        request.setEntity(new StringEntity("I'm an entity"));
        request.addHeader("foo", "bar");
        request.addHeader("content-length", "0");

        HttpCoreContext context = new HttpCoreContext();
        context.setTargetHost(HttpHost.create("localhost"));

        interceptor.process(request, context);

        Assert.assertEquals("bar", request.getFirstHeader("foo").getValue());
        Assert.assertEquals("wuzzle", request.getFirstHeader("Signature").getValue());
        Assert.assertNull(request.getFirstHeader("content-length"));
    }

    @Test
    void testBadRequest() throws Exception {
        HttpRequest badRequest = new BasicHttpRequest("GET", "?#!@*%");
        Assert.assertThrows(IOException.class, () -> {
            interceptor.process(badRequest, new BasicHttpContext());
        });
    }

    @Test
    void testEncodedUriSigner() throws Exception {
        HttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest(
                new MockRequestLine("/foo-2017-02-25%2Cfoo-2017-02-26/_search?a=b"));
        request.setEntity(new StringEntity("I'm an entity"));
        request.addHeader("foo", "bar");
        request.addHeader("content-length", "0");

        HttpCoreContext context = new HttpCoreContext();
        context.setTargetHost(HttpHost.create("localhost"));

        interceptor.process(request, context);

        Assert.assertEquals("bar", request.getFirstHeader("foo").getValue());
        Assert.assertEquals("wuzzle", request.getFirstHeader("Signature").getValue());
        Assert.assertNotNull(request.getFirstHeader("content-length"));
        Assert.assertEquals("/foo-2017-02-25%2Cfoo-2017-02-26/_search", request.getFirstHeader("resourcePath").getValue());
    }

    private static class AddHeaderSigner implements Signer {
        private final String name;
        private final String value;

        private AddHeaderSigner(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public SdkHttpFullRequest sign(SdkHttpFullRequest request, ExecutionAttributes ea) {
            if(request.contentStreamProvider().isPresent()){
                ContentStreamProvider contentStreamProvider = request.contentStreamProvider().get();
                return SdkHttpFullRequest.builder()
                        .uri(request.getUri())
                        .method(SdkHttpMethod.GET)
                        .contentStreamProvider(contentStreamProvider)
                        .headers(request.headers())
                        .appendHeader(name, value)
                        .appendHeader("resourcePath", request.getUri().getRawPath())
                        .build();
            }
            return null;
        }
    }

    private static class MockRequestLine implements RequestLine {
        private final String uri;

        public MockRequestLine(String uri) { this.uri = uri; }

        @Override public String getMethod() { return "GET"; }
        @Override public String getUri() { return uri; }

        @Override
        public ProtocolVersion getProtocolVersion() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
