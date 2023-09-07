/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineTransformer;
import org.opensearch.dataprepper.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

@Configuration
public class PipelineParserConfiguration {

    @Bean
    public PipelineTransformer pipelineParser(
            final PipelinesDataFlowModel pipelinesDataFlowModel,
            final PluginFactory pluginFactory,
            final PeerForwarderProvider peerForwarderProvider,
            final RouterFactory routerFactory,
            final DataPrepperConfiguration dataPrepperConfiguration,
            final CircuitBreakerManager circuitBreakerManager,
            final EventFactory eventFactory,
            final AcknowledgementSetManager acknowledgementSetManager,
            final SourceCoordinatorFactory sourceCoordinatorFactory
            ) {
        return new PipelineTransformer(pipelinesDataFlowModel,
                pluginFactory,
                peerForwarderProvider,
                routerFactory,
                dataPrepperConfiguration,
                circuitBreakerManager,
                eventFactory,
                acknowledgementSetManager,
                sourceCoordinatorFactory);
    }

    @Bean
    public PipelinesDataflowModelParser pipelinesDataflowModelParser(
            final FileStructurePathProvider fileStructurePathProvider) {
        return new PipelinesDataflowModelParser(fileStructurePathProvider.getPipelineConfigFileLocation());
    }

    @Bean
    public PipelinesDataFlowModel pipelinesDataFlowModel(
            final PipelinesDataflowModelParser pipelinesDataflowModelParser) {
        return pipelinesDataflowModelParser.parseConfiguration();
    }
}
