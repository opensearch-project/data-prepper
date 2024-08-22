/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineTransformer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.validation.PluginErrorCollector;
import org.opensearch.dataprepper.validation.PluginErrorsConsolidator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ExtendWith(MockitoExtension.class)
class PipelineParserConfigurationTest {
    private static final PipelineParserConfiguration pipelineParserConfiguration = new PipelineParserConfiguration();

    @Mock
    private PipelinesDataFlowModel pipelinesDataFlowModel;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    private RouterFactory routerFactory;

    @Mock
    private SourceCoordinatorFactory sourceCoordinatorFactory;

    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;

    @Mock
    private CircuitBreakerManager circuitBreakerManager;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginErrorCollector pluginErrorCollector;

    @Mock
    private PluginErrorsConsolidator pluginErrorsConsolidator;

    @Test
    void pipelineParser() {
        final PipelineTransformer pipelineTransformer = pipelineParserConfiguration.pipelineParser(
                pipelinesDataFlowModel, pluginFactory, peerForwarderProvider, routerFactory,
                dataPrepperConfiguration, circuitBreakerManager, eventFactory, acknowledgementSetManager,
                sourceCoordinatorFactory, pluginErrorCollector, pluginErrorsConsolidator);

        assertThat(pipelineTransformer, is(notNullValue()));
    }
}
