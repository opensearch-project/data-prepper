/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineParserConfigurationTest {
    private static final PipelineParserConfiguration pipelineParserConfiguration = new PipelineParserConfiguration();

    @Mock
    private DataPrepperArgs args;

    @Mock
    private PluginFactory pluginFactory;

    @Test
    void pipelineParser() {
        final String pipelineConfigFileLocation = "hot soup";
        when(args.getPipelineConfigFileLocation())
                .thenReturn(pipelineConfigFileLocation);

        final PipelineParser pipelineParser = pipelineParserConfiguration.pipelineParser(args, pluginFactory);

        assertThat(pipelineParser, is(notNullValue()));
        verify(args, times(1)).getPipelineConfigFileLocation();
    }
}