/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FailedHttpResponseInterceptorTest {

    private FailedHttpResponseInterceptor failedHttpResponseInterceptor;

    private  HttpResponse httpResponse;

    private EntityDetails entityDetails;

    private HttpContext httpContext;

    @Test
    public void test_process(){
        httpResponse = mock(HttpResponse.class);
        failedHttpResponseInterceptor = new FailedHttpResponseInterceptor("http://localhost:8080");
        when(httpResponse.getCode()).thenReturn(501);
        assertThrows(IOException.class, () -> failedHttpResponseInterceptor.process(httpResponse, entityDetails, httpContext));
    }
}
