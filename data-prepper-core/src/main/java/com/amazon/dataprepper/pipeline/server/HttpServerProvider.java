/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;

@Named
public class HttpServerProvider implements Provider<HttpServer> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpServerProvider.class);

    private final DataPrepperConfiguration dataPrepperConfiguration;

    @Inject
    public HttpServerProvider(final DataPrepperConfiguration dataPrepperConfiguration) {
        this.dataPrepperConfiguration = dataPrepperConfiguration;
    }

    @Override
    public HttpServer get() {
        final InetSocketAddress socketAddress = new InetSocketAddress(dataPrepperConfiguration.getServerPort());
        try {
            if (dataPrepperConfiguration.ssl()) {
                LOG.info("Creating Data Prepper server with TLS");
                final SSLContext sslContext = SslUtil.createSslContext(
                        dataPrepperConfiguration.getKeyStoreFilePath(),
                        dataPrepperConfiguration.getKeyStorePassword(),
                        dataPrepperConfiguration.getPrivateKeyPassword()
                );

                final HttpsServer server = HttpsServer.create(socketAddress, 0);
                server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                    public void configure(final HttpsParameters params) {
                        final SSLContext context = getSSLContext();
                        final SSLParameters sslParams = context.getDefaultSSLParameters();
                        params.setSSLParameters(sslParams);
                    }
                });

                return server;
            } else {
                return HttpServer.create(socketAddress, 0);
            }
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create server", ex);
        }
    }
}
