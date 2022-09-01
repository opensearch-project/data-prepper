/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.peerforwarder.PeerForwarder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineParserConfigurationTest {
    private static final PipelineParserConfiguration pipelineParserConfiguration = new PipelineParserConfiguration();

    @Mock
    private DataPrepperArgs args;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PeerForwarder peerForwarder;

    @Test
    void pipelineParser() {
        final String pipelineConfigFileLocation = "hot soup";
        when(args.getPipelineConfigFileLocation())
                .thenReturn(pipelineConfigFileLocation);

        final PipelineParser pipelineParser = pipelineParserConfiguration.pipelineParser(args, pluginFactory, peerForwarder);

        assertThat(pipelineParser, is(notNullValue()));
        verify(args).getPipelineConfigFileLocation();
    }
}