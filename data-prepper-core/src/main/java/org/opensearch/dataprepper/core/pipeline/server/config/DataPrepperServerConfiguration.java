/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server.config;

import org.opensearch.dataprepper.core.DataPrepper;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;
import org.opensearch.dataprepper.core.pipeline.server.DataPrepperCoreAuthenticationProvider;
import org.opensearch.dataprepper.core.pipeline.server.ShutdownHandler;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.core.pipeline.server.GetPipelinesHandler;
import org.opensearch.dataprepper.core.pipeline.server.ListPipelinesHandler;
import com.sun.net.httpserver.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class DataPrepperServerConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperServerConfiguration.class);

    private void printInsecurePluginModelWarning() {
        LOG.warn("Creating data prepper server without authentication. This is not secure.");
        LOG.warn("In order to set up Http Basic authentication for the data prepper server, " +
                "go here: https://github.com/opensearch-project/data-prepper/blob/main/docs/core_apis.md#authentication");
    }

    @Bean
    public PluginSetting pluginSetting(@Autowired(required = false) final PluginModel authentication) {
        if (authentication != null) {
            final String pluginName = authentication.getPluginName();
            if (pluginName.equals(DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
                printInsecurePluginModelWarning();
            }
            return new PluginSetting(pluginName, authentication.getPluginSettings());
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
    public Authenticator authenticator(final DataPrepperCoreAuthenticationProvider authenticationProvider) {
        return authenticationProvider.getAuthenticator();
    }

    @Bean
    public ListPipelinesHandler listPipelinesHandler(final PipelinesProvider pipelinesProvider) {
        return new ListPipelinesHandler(pipelinesProvider);
    }

    @Bean
    public ShutdownHandler shutdownHandler(final DataPrepper dataPrepper) {
        return new ShutdownHandler(dataPrepper);
    }

    @Bean
    public GetPipelinesHandler GetPipelinesHandler(final PipelinesProvider pipelinesProvider) {
        return new GetPipelinesHandler(pipelinesProvider);
    }
}
