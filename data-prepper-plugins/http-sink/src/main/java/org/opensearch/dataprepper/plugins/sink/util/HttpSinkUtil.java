/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.util;

import org.apache.hc.core5.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class HttpSinkUtil {

    public static URL getURLByUrlString(final String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static HttpHost getHttpHostByURL(final URL url) {
        final HttpHost targetHost;
        try {
            targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return targetHost;
    }


}
