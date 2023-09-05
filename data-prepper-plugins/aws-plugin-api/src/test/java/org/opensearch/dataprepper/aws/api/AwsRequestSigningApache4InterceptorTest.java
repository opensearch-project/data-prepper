/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AwsRequestSigningApache4InterceptorTest {

    @Mock
    private Signer signer;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Mock
    private HttpEntityEnclosingRequest httpRequest;

    @Mock
    private HttpContext httpContext;

    private AwsRequestSigningApache4Interceptor createObjectUnderTest() {
        return new AwsRequestSigningApache4Interceptor("es", signer, awsCredentialsProvider, Region.US_EAST_1);
    }

    @Test
    void invalidURI_throws_IOException() {

        final RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getUri()).thenReturn("http://invalid-uri.com/file[/].html\n");

        when(httpRequest.getRequestLine()).thenReturn(requestLine);

        final AwsRequestSigningApache4Interceptor objectUnderTest = new AwsRequestSigningApache4Interceptor("es", signer, awsCredentialsProvider, "us-east-1");

        assertThrows(IOException.class, () -> objectUnderTest.process(httpRequest, httpContext));
    }

    @Test
    void testHappyPath() throws IOException {
        final RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(requestLine.getUri()).thenReturn("http://localhost?param=test");
        when(httpRequest.getRequestLine()).thenReturn(requestLine);
        when(httpRequest.getAllHeaders()).thenReturn(new BasicHeader[]{
                new BasicHeader("test-name", "test-value"),
                new BasicHeader("content-length", "0")
        });

        final HttpEntity httpEntity = mock(HttpEntity.class);
        final InputStream inputStream = mock(InputStream.class);
        when(httpEntity.getContent()).thenReturn(inputStream);

        when((httpRequest).getEntity()).thenReturn(httpEntity);

        final HttpHost httpHost = HttpHost.create("http://localhost?param=test");
        when(httpContext.getAttribute(HttpCoreContext.HTTP_TARGET_HOST)).thenReturn(httpHost);

        final SdkHttpFullRequest signedRequest = mock(SdkHttpFullRequest.class);
        when(signedRequest.headers()).thenReturn(Map.of("test-name", List.of("test-value")));
        final ContentStreamProvider contentStreamProvider = mock(ContentStreamProvider.class);
        final InputStream contentInputStream = mock(InputStream.class);
        when(contentStreamProvider.newStream()).thenReturn(contentInputStream);
        when(signedRequest.contentStreamProvider()).thenReturn(Optional.of(contentStreamProvider));
        when(signer.sign(any(SdkHttpFullRequest.class), any(ExecutionAttributes.class)))
                .thenReturn(signedRequest);
        createObjectUnderTest().process(httpRequest, httpContext);
    }
}
