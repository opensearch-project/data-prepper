package com.amazon.dataprepper.pipeline.server.config;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import com.amazon.dataprepper.pipeline.server.ListPipelinesHandler;
import com.amazon.dataprepper.pipeline.server.ShutdownHandler;
import com.amazon.dataprepper.pipeline.server.SslUtil;
import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;

@Configuration
public class DataPrepperServerConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServerConfiguration.class);

    @Bean
    public HttpServer httpServer(final DataPrepperConfiguration dataPrepperConfiguration) {
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

    private void printInsecurePluginModelWarning() {
        LOG.warn("Creating data prepper server without authentication. This is not secure.");
        LOG.warn("In order to set up Http Basic authentication for the data prepper server, " +
                "go here: https://github.com/opensearch-project/data-prepper/blob/main/docs/core_apis.md#authentication");
    }

    @Bean
    public PluginSetting pluginSetting(final Optional<PluginModel> optionalPluginModel) {
        if (optionalPluginModel.isPresent()) {
            final PluginModel pluginModel = optionalPluginModel.get();
            final String pluginName = pluginModel.getPluginName();
            if (pluginName.equals(DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
                printInsecurePluginModelWarning();
            }
            return new PluginSetting(pluginName, pluginModel.getPluginSettings());
        }
        else {
            printInsecurePluginModelWarning();
            return new PluginSetting(
                    DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
                    Collections.emptyMap());
        }
    }

    @Bean
    public DataPrepperCoreAuthenticationProvider authenticationProvider(
            final PluginFactory pluginFactory,
            final PluginSetting pluginSetting
    ) {
        return pluginFactory.loadPlugin(
                DataPrepperCoreAuthenticationProvider.class,
                pluginSetting
        );
    }

    @Bean
    public Optional<Authenticator> optionalAuthenticator(final DataPrepperCoreAuthenticationProvider authenticationProvider) {
        return Optional.ofNullable(authenticationProvider.getAuthenticator());
    }

    @Bean
    public ListPipelinesHandler listPipelinesHandler(
            final DataPrepper dataPrepper,
            final Optional<Authenticator> optionalAuthenticator,
            final HttpServer server
    ) {
        final ListPipelinesHandler listPipelinesHandler = new ListPipelinesHandler(dataPrepper);

        final HttpContext context = server.createContext("/list", listPipelinesHandler);
        optionalAuthenticator.ifPresent(context::setAuthenticator);

        return listPipelinesHandler;
    }

    @Bean
    public ShutdownHandler shutdownHandler(
            final DataPrepper dataPrepper,
            final Optional<Authenticator> optionalAuthenticator,
            final HttpServer server
    ) {
        final ShutdownHandler shutdownHandler = new ShutdownHandler(dataPrepper);

        final HttpContext context = server.createContext("/shutdown", shutdownHandler);
        optionalAuthenticator.ifPresent(context::setAuthenticator);

        return shutdownHandler;
    }
}
