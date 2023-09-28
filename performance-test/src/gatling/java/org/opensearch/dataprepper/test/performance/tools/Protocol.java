/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.http.HttpDsl;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Protocol {
    private Protocol() {
    }

    private static final String HTTP = "http";
    private static final String HTTPS = "https";

    private static final String HTTP_PROTOCOL = System.getProperty("protocol", HTTP);

    private static final String LOCALHOST = "localhost";

    private static final Integer DEFAULT_PORT = 2021;
    private static final Integer PORT = Integer.getInteger("port", DEFAULT_PORT);

    private static List<String> loadHosts() {
        String host = System.getProperty("host", LOCALHOST);

        return Arrays.asList(host.split(","));
    }

    private static String asUrl(final String protocol, final String host, final Integer port) {
        return protocol + "://" + host + ":" + port;
    }
    private static List<String> asUrls(final String protocol, final List<String> host, final Integer port) {
        return host.stream()
                .map(h -> asUrl(protocol, h, port))
                .collect(Collectors.toList());
    }

    public static HttpProtocolBuilder httpProtocol() {
        return httpProtocol(HTTP_PROTOCOL, loadHosts(), PORT);
    }

    public static HttpProtocolBuilder httpsProtocol(final Integer port) {
        return httpProtocol(HTTPS, loadHosts(), port);
    }

    private static HttpProtocolBuilder httpProtocol(final String protocol, final List<String> hosts, final Integer port) {
        return HttpDsl.http
                .baseUrls(asUrls(protocol, hosts, port))
                .sign(SignerProvider.getSigner())
                .acceptHeader("application/json")
                .header("Content-Type", "application/json");
    }
}
