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

    private static final String HTTP = "http";
    private static final String HTTPS = "https";

    private static final String HTTP_PROTOCOL = System.getProperty("protocol", HTTP);

    public static final String LOCALHOST = "localhost";
    private static final String HOST = System.getProperty("host", LOCALHOST);

    private static final Integer defaultPort = 2021;
    private static final Integer port = Integer.getInteger("port", defaultPort);

    private static String asUrl(final String protocol, final String host, final Integer port) {
        return protocol + "://" + host + ":" + port;
    }

    public static HttpProtocolBuilder httpProtocol() {
        return httpProtocol(HTTP_PROTOCOL, HOST, port);
    }

    public static HttpProtocolBuilder httpsProtocol(final Integer port) {
        return httpProtocol(HTTPS, HOST, port);
    }

    private static HttpProtocolBuilder httpProtocol(final String protocol, final String host, final Integer port) {
        return HttpDsl.http
                .baseUrl(asUrl(protocol, host, port))
                .sign(SignerProvider.getSigner())
                .acceptHeader("application/json")
                .header("Content-Type", "application/json");
    }
}
