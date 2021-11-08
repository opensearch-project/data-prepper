/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;

/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
public class DataPrepperServer {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServer.class);
    private HttpServer server;

    public DataPrepperServer(final DataPrepper dataPrepper) {
        final DataPrepperConfiguration dataPrepperConfiguration = DataPrepper.getConfiguration();
        final int port = dataPrepperConfiguration.getServerPort();
        final boolean ssl = dataPrepperConfiguration.ssl();
        final String keyStoreFilePath = dataPrepperConfiguration.getKeyStoreFilePath();
        final String keyStorePassword = dataPrepperConfiguration.getKeyStorePassword();
        final String privateKeyPassword = dataPrepperConfiguration.getPrivateKeyPassword();

        try {
            if (ssl) {
                LOG.info("Creating Data Prepper server with TLS");
                this.server = createHttpsServer(port, keyStoreFilePath, keyStorePassword, privateKeyPassword);
            } else {
                LOG.warn("Creating Data Prepper server without TLS. This is not secure.");
                LOG.warn("In order to set up TLS for the Data Prepper server, go here: https://github.com/opensearch-project/data-prepper/blob/main/docs/configuration.md#server-configuration");
                this.server = createHttpServer(port);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create server", e);
        }

        getPrometheusMeterRegistryFromRegistries(Metrics.globalRegistry.getRegistries()).ifPresent(meterRegistry -> {
            final PrometheusMeterRegistry prometheusMeterRegistryForDataPrepper = (PrometheusMeterRegistry) meterRegistry;
            server.createContext("/metrics/prometheus", new PrometheusMetricsHandler(prometheusMeterRegistryForDataPrepper));
        });

        getPrometheusMeterRegistryFromRegistries(DataPrepper.getSystemMeterRegistry().getRegistries()).ifPresent(
                meterRegistry -> {
                    final PrometheusMeterRegistry prometheusMeterRegistryForSystem = (PrometheusMeterRegistry) meterRegistry;
                    server.createContext("/metrics/sys", new PrometheusMetricsHandler(prometheusMeterRegistryForSystem));
                });
        server.createContext("/list", new ListPipelinesHandler(dataPrepper));
        server.createContext("/shutdown", new ShutdownHandler(dataPrepper));
    }

    private Optional<MeterRegistry> getPrometheusMeterRegistryFromRegistries(final Set<MeterRegistry> meterRegistries) {
        return meterRegistries.stream().filter(meterRegistry ->
                meterRegistry instanceof PrometheusMeterRegistry).findFirst();
    }

    /**
     * Start the DataPrepperServer
     */
    public void start() {
        server.start();
        LOG.info("Data Prepper server running at :{}", server.getAddress().getPort());

    }

    /**
     * Stop the DataPrepperServer
     */
    public void stop() {
        server.stop(0);
        LOG.info("Data Prepper server stopped");
    }

    private HttpServer createHttpServer(final int port) throws IOException {
        return HttpServer.create(new InetSocketAddress(port), 0);
    }

    private HttpServer createHttpsServer(final int port,
                                         final String keyStoreFilePath,
                                         final String keyStorePassword,
                                         final String privateKeyPassword) throws IOException {
        final SSLContext sslContext = SslUtil.createSslContext(keyStoreFilePath, keyStorePassword, privateKeyPassword);

        final HttpsServer server = HttpsServer.create(new InetSocketAddress(port), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            public void configure(HttpsParameters params) {
                SSLContext context = getSSLContext();
                SSLParameters sslparams = context.getDefaultSSLParameters();
                params.setSSLParameters(sslparams);
            }
        });

        return server;
    }
}
