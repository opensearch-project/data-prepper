/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PipelineParserConfiguration {

    @Bean
    public PipelineParser pipelineParser(
            final FileStructurePathProvider fileStructurePathProvider,
            final PluginFactory pluginFactory,
            final PeerForwarderProvider peerForwarderProvider,
            final RouterFactory routerFactory,
            final DataPrepperConfiguration dataPrepperConfiguration,
            final CircuitBreakerManager circuitBreakerManager,
            final SourceCoordinatorFactory sourceCoordinatorFactory
            ) {
        return new PipelineParser(fileStructurePathProvider.getPipelineConfigFileLocation(),
                pluginFactory,
                peerForwarderProvider,
                routerFactory,
                dataPrepperConfiguration,
                circuitBreakerManager,
                sourceCoordinatorFactory);
    }
}
