package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@ComponentScan(basePackageClasses = com.amazon.dataprepper.DataPrepper.class)
public class DataPrepperAppConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperAppConfiguration.class);
    private static final String POSITIONAL_COMMAND_LINE_ARGUMENTS = "nonOptionArgs";
    private static final String COMMAND_LINE_ARG_DELIMITER = ",";

    @Bean
    public DataPrepperArgs dataPrepperArgs(final Environment environment) {
        final String commandLineArgs = environment.getProperty(POSITIONAL_COMMAND_LINE_ARGUMENTS);

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

    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        if (configurationFileLocation.endsWith(".conf")) {
            final LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
            final Path configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath().getParent();

            try {
                configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                        configurationFileLocation, String.valueOf(configurationDirectory));
            } catch (IOException e) {
                LOG.error("Unable to read the Logstash configuration file", e);
            }
        }
        return configurationFileLocation;
    }
}
