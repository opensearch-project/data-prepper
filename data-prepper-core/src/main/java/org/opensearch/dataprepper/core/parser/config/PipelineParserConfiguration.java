/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.core.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.core.parser.PipelineTransformer;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.core.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.core.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineConfigurationTransformer;
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
            final PluginErrorCollector pluginErrorCollector,
            final PluginErrorsHandler pluginErrorsHandler,
            final ExpressionEvaluator expressionEvaluator
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
                pluginErrorCollector,
                pluginErrorsHandler,
                expressionEvaluator);
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
