/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
    private PluginErrorsHandler pluginErrorsHandler;

    @Test
    void pipelineParser() {
        final PipelineTransformer pipelineTransformer = pipelineParserConfiguration.pipelineParser(
                pipelinesDataFlowModel, pluginFactory, peerForwarderProvider, routerFactory,
                dataPrepperConfiguration, circuitBreakerManager, eventFactory, acknowledgementSetManager,
                sourceCoordinatorFactory, pluginErrorCollector, pluginErrorsHandler);

        assertThat(pipelineTransformer, is(notNullValue()));
    }
}
