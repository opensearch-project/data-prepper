/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Standalone Armeria HTTP server for serving shuffle data.
 * Runs independently from PeerForwarder to avoid core dependencies.
 */
public class ShuffleHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleHttpServer.class);

    private final ShuffleConfig config;
    private final ShuffleHttpService service;
    private Server server;

    public ShuffleHttpServer(final ShuffleConfig config, final ShuffleHttpService service) {
        this.config = config;
        this.service = service;
    }

    public void start() {
        final ServerBuilder sb = Server.builder();
        sb.disableServerHeader();

        if (config.isSsl()) {
            try {
                final String cert = Files.readString(Path.of(config.getSslCertificateFile()));
                final String key = Files.readString(Path.of(config.getSslKeyFile()));
                sb.https(config.getServerPort())
                        .tls(new ByteArrayInputStream(cert.getBytes(StandardCharsets.UTF_8)),
                             new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8)));
            } catch (final Exception e) {
                throw new RuntimeException("Failed to configure TLS for shuffle server", e);
            }
        } else {
            sb.http(config.getServerPort());
        }

        sb.annotatedService("/shuffle", service);

        server = sb.build();
        final CompletableFuture<Void> future = server.start();
        future.join();
        LOG.info("Shuffle HTTP server started on port {}", config.getServerPort());
    }

    public void stop() {
        if (server != null) {
            server.stop().join();
            LOG.info("Shuffle HTTP server stopped");
        }
    }

    public int getPort() {
        return config.getServerPort();
    }
}
