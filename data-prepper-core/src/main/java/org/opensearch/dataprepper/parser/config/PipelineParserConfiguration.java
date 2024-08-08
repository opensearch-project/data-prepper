/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineTransformer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineConfigurationTransformer;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.validation.PluginErrorCollector;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ComponentScan(basePackages = {
        "org.opensearch.dataprepper.pipeline.parser",
        "org.opensearch.dataprepper.plugin"
})
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
            final SourceCoordinatorFactory sourceCoordinatorFactory,
            final PluginErrorCollector pluginErrorCollector
            ) {
        return new PipelineTransformer(pipelinesDataFlowModel,
                pluginFactory,
                peerForwarderProvider,
                routerFactory,
                dataPrepperConfiguration,
                circuitBreakerManager,
                eventFactory,
                acknowledgementSetManager,
                sourceCoordinatorFactory,
                pluginErrorCollector);
    }

    @Bean
    public PipelineConfigurationReader pipelineConfigurationReader(
            final FileStructurePathProvider fileStructurePathProvider) {
        return new PipelineConfigurationFileReader(fileStructurePathProvider.getPipelineConfigFileLocation());
    }

    @Bean
    public PipelinesDataflowModelParser pipelinesDataflowModelParser(
            final PipelineConfigurationReader pipelineConfigurationReader) {
        return new PipelinesDataflowModelParser(pipelineConfigurationReader);
    }

    @Bean
    public PipelineConfigurationTransformer pipelineConfigTransformer(RuleEvaluator ruleEvaluator) {
        return new DynamicConfigTransformer(ruleEvaluator);
    }


    @Bean(name  = "pipelinesDataFlowModel")
    @Primary
    public PipelinesDataFlowModel pipelinesDataFlowModel(
            PipelineConfigurationTransformer pipelineConfigTransformer,
            @Qualifier("preTransformedDataFlowModel") PipelinesDataFlowModel preTransformedDataFlowModel) {
        return pipelineConfigTransformer.transformConfiguration(preTransformedDataFlowModel);
    }

    @Bean(name = "preTransformedDataFlowModel")
    public PipelinesDataFlowModel preTransformedDataFlowModel(
            final PipelinesDataflowModelParser pipelinesDataflowModelParser) {
        return pipelinesDataflowModelParser.parseConfiguration();
    }
}
