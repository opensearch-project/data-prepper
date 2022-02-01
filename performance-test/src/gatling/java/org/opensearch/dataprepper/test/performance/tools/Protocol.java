/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.http.HttpDsl;
import io.gatling.javaapi.http.HttpProtocolBuilder;

public final class Protocol {
    private Protocol() {
    }

    private static final String http = "http";
    private static final String https = "https";

    public static final String localhost = "localhost";
    private static final String host = System.getProperty("host", localhost);

    private static final Integer defaultPort = 2021;
    private static final Integer port = Integer.getInteger("port", defaultPort);

    private static String asUrl(final String protocol, final String host, final Integer port) {
        return protocol + "://" + host + ":" + port;
    }

    public static HttpProtocolBuilder httpProtocol() {
        return httpProtocol(http, host, port);
    }

    public static HttpProtocolBuilder httpProtocol(final String host) {
        return httpProtocol(http, host, port);
    }

    public static HttpProtocolBuilder httpsProtocol() {
        return httpProtocol(https, host, port);
    }

    public static HttpProtocolBuilder httpsProtocol(final String host) {
        return httpProtocol(https, host, port);
    }

    public static HttpProtocolBuilder httpsProtocol(final Integer port) {
        return httpProtocol(https, host, port);
    }

    public static HttpProtocolBuilder httpsProtocol(final String host, final Integer port) {
        return httpProtocol(https, host, port);
    }

    public static HttpProtocolBuilder httpProtocol(final String protocol, final String host) {
        return httpProtocol(protocol, host, port);
    }

    public static HttpProtocolBuilder httpProtocol(final String protocol, final String host, final Integer port) {
        return HttpDsl.http
                .baseUrl(asUrl(protocol, host, port))
                .acceptHeader("application/json")
                .header("Content-Type", "application/json");
    }
}
