/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PipelineParserConfiguration {

    @Bean
    public PipelineParser pipelineParser(final DataPrepperArgs dataPrepperArgs, final PluginFactory pluginFactory) {
        return new PipelineParser(dataPrepperArgs.getPipelineConfigFileLocation(), pluginFactory);
    }
}
