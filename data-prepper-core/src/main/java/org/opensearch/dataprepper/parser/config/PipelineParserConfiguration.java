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
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Qualifier;

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
    public PipelinesDataFlowModel pipelinesDataFlowModel(
            @Qualifier("preTransformedDataFlowModel") PipelinesDataFlowModel preTransformedDataFlowModel,
            DynamicConfigTransformer pipelineConfigTransformer,
            TransformersFactory transformersFactory) {
        transformersFactory.getTemplateModel("documentdb");
        return pipelineConfigTransformer.transformConfiguration(preTransformedDataFlowModel, templateModel);
    }

    @Bean(name = "preTransformedDataFlowModel")
    public PipelinesDataFlowModel preTransformedDataFlowModel(
            final PipelinesDataflowModelParser pipelinesDataflowModelParser) {
        return pipelinesDataflowModelParser.parseConfiguration();
    }

//    @Bean
//    public PipelineTemplateModel pipelineTemplateModel(
//            final PipelineTransformationPathProvider transformationResourcesPathProvider){
//        return transformationResourcesPathProvider.get();
//    }

//    @Bean
//    public PipelineTemplateModel pipelineTemplateModel(
//            final PipelineTransformationPathProvider transformationResourcesPathProvider){
//        return transformationResourcesPathProvider.getTemplateModel();
//    }
//
//    @Bean
//    public PipelineTemplateModel pipelineTemplateModel(
//            final PipelineTransformationPathProvider transformationResourcesPathProvider){
//        return transformationResourcesPathProvider.getTemplateModel();
//    }
//
//    @Bean
//    public PipelineTransformationPathProvider transformationResourcesPathProvider(
//            final PipelineTransformationPathProvider transformationResourcesPathProvider){
//        return transformationResourcesPathProvider.getTemplateModel();
//    }

}
