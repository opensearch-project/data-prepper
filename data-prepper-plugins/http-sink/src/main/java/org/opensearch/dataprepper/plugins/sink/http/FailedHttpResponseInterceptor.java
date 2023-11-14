/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpResponseInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;

import java.io.IOException;

public class FailedHttpResponseInterceptor implements HttpResponseInterceptor {

    public static final int STATUS_CODE_200 = 200;

    private final String url;

    public FailedHttpResponseInterceptor(final String url){
        this.url = url;
    }

    @Override
    public void process(HttpResponse response, EntityDetails entity, HttpContext context) throws IOException {
        if (response.getCode() != STATUS_CODE_200) {
            throw new IOException(String.format("url:  %s , status code: %s", url,response.getCode()));
        }
    }
}
