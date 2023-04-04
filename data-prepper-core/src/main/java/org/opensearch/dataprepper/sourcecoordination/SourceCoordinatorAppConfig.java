/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourceCoordinatorAppConfig {

    @Bean
    public SourceCoordinationConfig sourceCoordinationConfig(@Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getSourceCoordinationConfig() != null) {
            return dataPrepperConfiguration.getSourceCoordinationConfig();
        }

        return null;
    }

    @Bean
    public SourceCoordinatorFactory provideSourceCoordinatorFactory(@Autowired(required = false) final SourceCoordinationConfig sourceCoordinationConfig,
                                                                    final PluginFactory pluginFactory) {
        return new SourceCoordinatorFactory(sourceCoordinationConfig, pluginFactory);
    }
}
