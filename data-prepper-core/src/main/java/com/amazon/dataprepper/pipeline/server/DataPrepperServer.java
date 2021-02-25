package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;

/**
 * Class to handle any serving that the data prepper instance needs to do.
 * Currently, only serves metrics in prometheus format.
 */
public class DataPrepperServer {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServer.class);
    private final SslUtil sslUtil = new SslUtil();
    private HttpServer server;

    public DataPrepperServer(final DataPrepper dataPrepper) {
        final int port = DataPrepper.getConfiguration().getServerPort();
        final boolean ssl = DataPrepper.getConfiguration().ssl();
        final String sslKeyFilePath = DataPrepper.getConfiguration().getSslKeyFile();
        final String sslKeyCertChainFilePath = DataPrepper.getConfiguration().getSslKeyCertChainFile();

        try {
            if (ssl) {
                LOG.info("Creating Data Prepper server with TLS");
                this.server = createHttpsServer(port, sslKeyFilePath, sslKeyCertChainFilePath);
            } else {
                LOG.info("Creating Data Prepper server without TLS");
                this.server = createHttpServer(port);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create server", e);
        }

        server.createContext("/metrics/prometheus", new PrometheusMetricsHandler());
        server.createContext("/metrics/sys", new PrometheusMetricsHandler(DataPrepper.getSysJVMMeterRegistry()));
        server.createContext("/list", new ListPipelinesHandler(dataPrepper));
        server.createContext("/shutdown", new ShutdownHandler(dataPrepper));
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
                                         final String sslKeyFilePath,
                                         final String sslKeyCertChainFilePath) throws IOException {
        final SSLContext sslContext;

        try {
            KeyStore keyStore = sslUtil.loadKeyStore(new File(sslKeyCertChainFilePath), new File(sslKeyFilePath));

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            char[] passphraseArray = "".toCharArray();
            keyManagerFactory.init(keyStore, passphraseArray);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        } catch (Exception e) {
            throw new IllegalStateException("Problem loading keystore to enable TLS", e);
        }

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
