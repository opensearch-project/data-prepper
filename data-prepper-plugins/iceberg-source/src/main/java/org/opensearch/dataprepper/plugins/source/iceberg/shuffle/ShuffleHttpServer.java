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
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
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
                final CertificateProvider certificateProvider = createCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(config.getServerPort())
                        .tls(new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                             new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
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

    private CertificateProvider createCertificateProvider() {
        final String certFile = config.getSslCertificateFile();
        final String keyFile = config.getSslKeyFile();
        if (certFile.toLowerCase().startsWith(S3_PREFIX) && keyFile.toLowerCase().startsWith(S3_PREFIX)) {
            LOG.info("Loading SSL certificates from S3");
            final S3Client s3Client = S3Client.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(new DefaultAwsRegionProviderChain().getRegion())
                    .build();
            return new S3CertificateProvider(s3Client, certFile, keyFile);
        }
        LOG.info("Loading SSL certificates from local filesystem");
        return new FileCertificateProvider(certFile, keyFile);
    }

    private static final String S3_PREFIX = "s3://";
}
