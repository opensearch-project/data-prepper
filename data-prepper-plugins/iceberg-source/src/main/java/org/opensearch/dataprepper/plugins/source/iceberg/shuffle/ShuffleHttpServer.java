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
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * HTTP server for serving shuffle data to other Data Prepper nodes.
 */
public class ShuffleHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleHttpServer.class);

    private final ShuffleConfig config;
    private final ShuffleHttpService service;
    private final Certificate certificate;
    private Server server;

    public ShuffleHttpServer(final ShuffleConfig config, final ShuffleHttpService service) {
        this.config = config;
        this.service = service;
        // Load the TLS certificate once at construction time. The same Certificate object is used
        // for the server TLS configuration and shared with ShuffleNodeClient (via getCertificate())
        // for client-side mTLS authentication.
        this.certificate = config.isSsl()
                ? new CertificateProviderFactory(config).getCertificateProvider().getCertificate()
                : null;
    }

    public void start() {
        final ServerConfiguration serverConfig = new ServerConfiguration();
        serverConfig.setPort(config.getServerPort());
        serverConfig.setSsl(config.isSsl());

        final CertificateProvider certificateProvider = certificate != null
                ? () -> certificate
                : null;

        final CreateServer createServer = new CreateServer(
                serverConfig, LOG, PluginMetrics.fromNames("shuffle", "iceberg-source"),
                "shuffle-server", "iceberg-cdc-pipeline");

        server = createServer.createHTTPServer(
                certificateProvider, null, service, "/shuffle", config.isSslClientAuth());
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

    public Certificate getCertificate() {
        return certificate;
    }

    public int getPort() {
        return config.getServerPort();
    }
}
