/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core;

import org.opensearch.dataprepper.core.parser.config.DataPrepperAppConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.CommandLinePropertySource;
import org.springframework.core.env.Environment;

@Configuration
class DataPrepperArgumentConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperAppConfiguration.class);
    private static final String COMMAND_LINE_ARG_DELIMITER = ",";

    @Bean
    public DataPrepperArgs dataPrepperArgs(final Environment environment) {
        final String commandLineArgs = environment.getProperty(CommandLinePropertySource.DEFAULT_NON_OPTION_ARGS_PROPERTY_NAME);

        LOG.info("Command line args: {}", commandLineArgs);

        if (commandLineArgs != null) {
            final String[] args = commandLineArgs.split(COMMAND_LINE_ARG_DELIMITER);
            return new DataPrepperArgs(args);
        }
        else {
            throw new RuntimeException("Configuration file command line argument required but none found");
        }
    }
}
