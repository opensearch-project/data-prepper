/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.CommandLinePropertySource;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;

@Configuration
public class DataPrepperAppConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperAppConfiguration.class);
    private static final String COMMAND_LINE_ARG_DELIMITER = ",";

    @Bean
    public DataPrepperArgs dataPrepperArgs(final Environment environment) {
        final String commandLineArgs = environment.getProperty(CommandLinePropertySource.DEFAULT_NON_OPTION_ARGS_PROPERTY_NAME);

        LOG.info("Command line args: {}", commandLineArgs);

        if (commandLineArgs != null) {
            String[] args = commandLineArgs.split(COMMAND_LINE_ARG_DELIMITER);
            return new DataPrepperArgs(args);
        }
        else {
            throw new RuntimeException("Configuration file command line argument required but none found");
        }
    }

    @Bean
    public DataPrepperConfiguration dataPrepperConfiguration(
            final DataPrepperArgs dataPrepperArgs,
            final ObjectMapper objectMapper
    ) {
        final String dataPrepperConfigFileLocation = dataPrepperArgs.getDataPrepperConfigFileLocation();
        if (dataPrepperConfigFileLocation != null) {
            final File configurationFile = new File(dataPrepperConfigFileLocation);
            try {
                return objectMapper.readValue(configurationFile, DataPrepperConfiguration.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid DataPrepper configuration file.", e);
            }
        }
        else {
            return new DataPrepperConfiguration();
        }
    }
}
